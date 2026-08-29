#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import process from "node:process";

import { parseArgs } from "./lib/args.mjs";
import { BROKER_BUSY_RPC_CODE, CodexAppServerClient } from "./lib/app-server.mjs";
import { parseBrokerEndpoint } from "./lib/broker-endpoint.mjs";

const STREAMING_METHODS = new Set(["turn/start", "review/start", "thread/compact/start"]);
const SUBSCRIBING_METHODS = new Set(["thread/start", "thread/resume", "thread/fork"]);

function buildSubscriptionThreadIds(method, result) {
  const threadIds = new Set();
  if (SUBSCRIBING_METHODS.has(method) && result?.thread?.id) {
    threadIds.add(result.thread.id);
  }
  if (method === "review/start" && result?.reviewThreadId) {
    threadIds.add(result.reviewThreadId);
  }
  return threadIds;
}

function buildProvisionalSubscriptionThreadIds(method, params) {
  const threadIds = new Set();
  if (method === "thread/resume" && params?.threadId) {
    threadIds.add(params.threadId);
  }
  if (method === "review/start" && params?.threadId) {
    threadIds.add(params.threadId);
  }
  return threadIds;
}

function buildNotificationSubscriptionThreadIds(message) {
  const threadIds = new Set();
  const params = message?.params;
  if (message?.method === "thread/started" && params?.thread?.id) {
    threadIds.add(params.thread.id);
  }
  if (params?.threadId) {
    threadIds.add(params.threadId);
  }
  if (Array.isArray(params?.item?.receiverThreadIds)) {
    for (const threadId of params.item.receiverThreadIds) {
      if (threadId) {
        threadIds.add(threadId);
      }
    }
  }
  return threadIds;
}

function buildStreamThreadIds(method, params, result) {
  const threadIds = new Set();
  if (params?.threadId) {
    threadIds.add(params.threadId);
  }
  if (method === "review/start" && result?.reviewThreadId) {
    threadIds.add(result.reviewThreadId);
  }
  return threadIds;
}

function buildJsonRpcError(code, message, data) {
  return data === undefined ? { code, message } : { code, message, data };
}

function send(socket, message) {
  if (socket.destroyed) {
    return;
  }
  socket.write(`${JSON.stringify(message)}\n`);
}

function isInterruptRequest(message) {
  return message?.method === "turn/interrupt";
}

function writePidFile(pidFile) {
  if (!pidFile) {
    return;
  }
  fs.mkdirSync(path.dirname(pidFile), { recursive: true });
  fs.writeFileSync(pidFile, `${process.pid}\n`, "utf8");
}

async function main() {
  const [subcommand, ...argv] = process.argv.slice(2);
  if (subcommand !== "serve") {
    throw new Error("Usage: node scripts/app-server-broker.mjs serve --endpoint <value> [--cwd <path>] [--pid-file <path>]");
  }

  const { options } = parseArgs(argv, {
    valueOptions: ["cwd", "pid-file", "endpoint"]
  });

  if (!options.endpoint) {
    throw new Error("Missing required --endpoint.");
  }

  const cwd = options.cwd ? path.resolve(process.cwd(), options.cwd) : process.cwd();
  const endpoint = String(options.endpoint);
  const listenTarget = parseBrokerEndpoint(endpoint);
  const pidFile = options["pid-file"] ? path.resolve(options["pid-file"]) : null;
  writePidFile(pidFile);

  const appClient = await CodexAppServerClient.connect(cwd, { disableBroker: true });
  let activeRequestSocket = null;
  let activeStreamSocket = null;
  let activeStreamThreadIds = null;
  const sockets = new Set();
  // App-server subscriptions belong to the broker's single upstream connection.
  // Mirror downstream ownership so one client cannot release another client's thread.
  const socketThreadIds = new Map();
  const threadSockets = new Map();
  const pendingUnsubscribes = new Map();
  const socketUnsubscribingThreadIds = new Map();

  function addThreadOwner(socket, threadId) {
    let ownedThreadIds = socketThreadIds.get(socket);
    if (!ownedThreadIds) {
      ownedThreadIds = new Set();
      socketThreadIds.set(socket, ownedThreadIds);
    }
    if (ownedThreadIds.has(threadId)) {
      return false;
    }
    ownedThreadIds.add(threadId);

    let owners = threadSockets.get(threadId);
    if (!owners) {
      owners = new Set();
      threadSockets.set(threadId, owners);
    }
    owners.add(socket);
    return true;
  }

  function removeThreadOwner(socket, threadId) {
    const ownedThreadIds = socketThreadIds.get(socket);
    if (!ownedThreadIds?.delete(threadId)) {
      return false;
    }
    if (ownedThreadIds.size === 0) {
      socketThreadIds.delete(socket);
    }

    const owners = threadSockets.get(threadId);
    owners?.delete(socket);
    if (owners?.size === 0) {
      threadSockets.delete(threadId);
    }
    return true;
  }

  function setSocketThreadUnsubscribing(socket, threadId, isUnsubscribing) {
    let threadIds = socketUnsubscribingThreadIds.get(socket);
    if (isUnsubscribing) {
      if (!threadIds) {
        threadIds = new Set();
        socketUnsubscribingThreadIds.set(socket, threadIds);
      }
      threadIds.add(threadId);
      return;
    }
    threadIds?.delete(threadId);
    if (threadIds?.size === 0) {
      socketUnsubscribingThreadIds.delete(socket);
    }
  }

  function requestThreadUnsubscribe(threadId) {
    const pending = pendingUnsubscribes.get(threadId);
    if (pending) {
      return pending;
    }
    const request = appClient.request("thread/unsubscribe", { threadId }).then(
      (result) => ({ result, error: null }),
      (error) => {
        process.stderr.write(
          `Failed to unsubscribe Codex thread ${threadId}: ${error instanceof Error ? error.message : String(error)}\n`
        );
        return { result: null, error };
      }
    );
    pendingUnsubscribes.set(threadId, request);
    void request.finally(() => {
      if (pendingUnsubscribes.get(threadId) === request) {
        pendingUnsubscribes.delete(threadId);
      }
    });
    return request;
  }

  async function unsubscribeIfUnowned(threadId) {
    if (threadSockets.has(threadId) || appClient.closed) {
      return null;
    }
    return requestThreadUnsubscribe(threadId);
  }

  async function releaseThreadOwners(socket, threadIds = socketThreadIds.get(socket) ?? new Set()) {
    const releasedThreadIds = [];
    for (const threadId of [...threadIds]) {
      if (removeThreadOwner(socket, threadId) && !threadSockets.has(threadId)) {
        releasedThreadIds.push(threadId);
      }
    }
    await Promise.all(releasedThreadIds.map((threadId) => unsubscribeIfUnowned(threadId)));
  }

  function trackSubscriptionResults(socket, method, result, provisionalThreadIds) {
    for (const threadId of buildSubscriptionThreadIds(method, result)) {
      if (provisionalThreadIds.has(threadId)) {
        continue;
      }
      if (socket.destroyed || !sockets.has(socket)) {
        void unsubscribeIfUnowned(threadId);
        continue;
      }
      addThreadOwner(socket, threadId);
    }
  }

  function trackNotificationSubscriptions(socket, message) {
    // App-server auto-subscribes its connection to child threads created by subagents.
    // Attribute those notification-only subscriptions to the active downstream client.
    for (const threadId of buildNotificationSubscriptionThreadIds(message)) {
      if (socket && !socket.destroyed && sockets.has(socket)) {
        if (!socketUnsubscribingThreadIds.get(socket)?.has(threadId)) {
          addThreadOwner(socket, threadId);
        }
      } else {
        void unsubscribeIfUnowned(threadId);
      }
    }
  }

  async function handleThreadUnsubscribe(socket, params) {
    const threadId = params?.threadId;
    if (typeof threadId !== "string") {
      return appClient.request("thread/unsubscribe", params ?? {});
    }

    const ownedThreadIds = socketThreadIds.get(socket);
    if (!ownedThreadIds?.has(threadId)) {
      if (threadSockets.has(threadId)) {
        return { status: "notSubscribed" };
      }
      setSocketThreadUnsubscribing(socket, threadId, true);
      try {
        const outcome = await requestThreadUnsubscribe(threadId);
        if (outcome.error) {
          throw outcome.error;
        }
        return outcome.result;
      } finally {
        setSocketThreadUnsubscribing(socket, threadId, false);
      }
    }

    removeThreadOwner(socket, threadId);
    if (threadSockets.has(threadId)) {
      return { status: "unsubscribed" };
    }

    setSocketThreadUnsubscribing(socket, threadId, true);
    try {
      const outcome = await unsubscribeIfUnowned(threadId);
      if (outcome?.error) {
        if (!socket.destroyed && sockets.has(socket)) {
          addThreadOwner(socket, threadId);
        }
        throw outcome.error;
      }
      return outcome?.result ?? { status: "unsubscribed" };
    } finally {
      setSocketThreadUnsubscribing(socket, threadId, false);
    }
  }

  function clearSocketOwnership(socket) {
    if (activeRequestSocket === socket) {
      activeRequestSocket = null;
    }
    if (activeStreamSocket === socket) {
      activeStreamSocket = null;
      activeStreamThreadIds = null;
    }
  }

  function routeNotification(message) {
    const target = activeRequestSocket ?? activeStreamSocket;
    trackNotificationSubscriptions(target, message);
    if (!target) {
      return;
    }
    send(target, message);
    if (message.method === "turn/completed" && activeStreamSocket === target) {
      const threadId = message.params?.threadId ?? null;
      if (!threadId || !activeStreamThreadIds || activeStreamThreadIds.has(threadId)) {
        activeStreamSocket = null;
        activeStreamThreadIds = null;
        if (activeRequestSocket === target) {
          activeRequestSocket = null;
        }
      }
    }
  }

  async function shutdown(server) {
    for (const socket of sockets) {
      socket.end();
    }
    await appClient.close().catch(() => {});
    await new Promise((resolve) => server.close(resolve));
    if (listenTarget.kind === "unix" && fs.existsSync(listenTarget.path)) {
      fs.unlinkSync(listenTarget.path);
    }
    if (pidFile && fs.existsSync(pidFile)) {
      fs.unlinkSync(pidFile);
    }
  }

  appClient.setNotificationHandler(routeNotification);

  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    let buffer = "";

    socket.on("data", async (chunk) => {
      buffer += chunk;
      let newlineIndex = buffer.indexOf("\n");
      while (newlineIndex !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        newlineIndex = buffer.indexOf("\n");

        if (!line.trim()) {
          continue;
        }

        let message;
        try {
          message = JSON.parse(line);
        } catch (error) {
          send(socket, {
            id: null,
            error: buildJsonRpcError(-32700, `Invalid JSON: ${error.message}`)
          });
          continue;
        }

        if (message.id !== undefined && message.method === "initialize") {
          send(socket, {
            id: message.id,
            result: {
              userAgent: "codex-companion-broker"
            }
          });
          continue;
        }

        if (message.method === "initialized" && message.id === undefined) {
          continue;
        }

        if (message.id !== undefined && message.method === "broker/shutdown") {
          send(socket, { id: message.id, result: {} });
          await shutdown(server);
          process.exit(0);
        }

        if (message.id === undefined) {
          continue;
        }

        const allowInterruptDuringActiveStream =
          isInterruptRequest(message) && activeStreamSocket && activeStreamSocket !== socket && !activeRequestSocket;

        if (
          ((activeRequestSocket && activeRequestSocket !== socket) || (activeStreamSocket && activeStreamSocket !== socket)) &&
          !allowInterruptDuringActiveStream
        ) {
          send(socket, {
            id: message.id,
            error: buildJsonRpcError(BROKER_BUSY_RPC_CODE, "Shared Codex broker is busy.")
          });
          continue;
        }

        if (allowInterruptDuringActiveStream) {
          try {
            const result = await appClient.request(message.method, message.params ?? {});
            send(socket, { id: message.id, result });
          } catch (error) {
            send(socket, {
              id: message.id,
              error: buildJsonRpcError(error.rpcCode ?? -32000, error.message)
            });
          }
          continue;
        }

        const isStreaming = STREAMING_METHODS.has(message.method);
        // Claim known thread ids before awaiting app-server. This prevents another
        // client's close handler from unsubscribing a concurrently resumed thread.
        const provisionalThreadIds = buildProvisionalSubscriptionThreadIds(message.method, message.params ?? {});
        const addedProvisionalThreadIds = new Set();
        for (const threadId of provisionalThreadIds) {
          if (addThreadOwner(socket, threadId)) {
            addedProvisionalThreadIds.add(threadId);
          }
        }
        activeRequestSocket = socket;

        try {
          const result =
            message.method === "thread/unsubscribe"
              ? await handleThreadUnsubscribe(socket, message.params ?? {})
              : await appClient.request(message.method, message.params ?? {});
          trackSubscriptionResults(socket, message.method, result, provisionalThreadIds);
          send(socket, { id: message.id, result });
          if (isStreaming) {
            activeStreamSocket = socket;
            activeStreamThreadIds = buildStreamThreadIds(message.method, message.params ?? {}, result);
          }
          if (activeRequestSocket === socket) {
            activeRequestSocket = null;
          }
        } catch (error) {
          await releaseThreadOwners(socket, addedProvisionalThreadIds);
          send(socket, {
            id: message.id,
            error: buildJsonRpcError(error.rpcCode ?? -32000, error.message)
          });
          if (activeRequestSocket === socket) {
            activeRequestSocket = null;
          }
          if (activeStreamSocket === socket && !isStreaming) {
            activeStreamSocket = null;
          }
        }
      }
    });

    socket.on("close", () => {
      sockets.delete(socket);
      clearSocketOwnership(socket);
      socketUnsubscribingThreadIds.delete(socket);
      void releaseThreadOwners(socket);
    });

    socket.on("error", () => {
      sockets.delete(socket);
      clearSocketOwnership(socket);
      socketUnsubscribingThreadIds.delete(socket);
      void releaseThreadOwners(socket);
    });
  });

  process.on("SIGTERM", async () => {
    await shutdown(server);
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    await shutdown(server);
    process.exit(0);
  });

  server.listen(listenTarget.path);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});
