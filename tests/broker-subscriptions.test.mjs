import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import test from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { makeTempDir } from "./helpers.mjs";

const ROOT = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
const BROKER = path.join(ROOT, "plugins", "codex", "scripts", "app-server-broker.mjs");

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, { timeoutMs = 10000, intervalMs = 25 } = {}) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return true;
    }
    await delay(intervalMs);
  }
  return false;
}

function readState(statePath) {
  try {
    return JSON.parse(fs.readFileSync(statePath, "utf8"));
  } catch {
    return null;
  }
}

function startBroker(behavior = "review-ok") {
  const binDir = makeTempDir("codex-broker-bin-");
  installFakeCodex(binDir, behavior);
  const sessionDir = makeTempDir("codex-broker-subscriptions-");
  const cwd = makeTempDir("codex-broker-cwd-");
  const socketPath = path.join(sessionDir, "broker.sock");
  const pidFile = path.join(sessionDir, "broker.pid");
  const statePath = path.join(binDir, "fake-codex-state.json");

  const child = spawn(
    process.execPath,
    [BROKER, "serve", "--endpoint", `unix:${socketPath}`, "--cwd", cwd, "--pid-file", pidFile],
    { env: buildEnv(binDir), stdio: ["ignore", "pipe", "pipe"] }
  );
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const exited = new Promise((resolve) => {
    child.on("exit", (code, signal) => resolve({ code, signal }));
  });

  async function stop() {
    if (child.exitCode !== null || child.signalCode !== null) {
      await exited;
      return;
    }
    child.kill("SIGTERM");
    const result = await Promise.race([exited, delay(5000).then(() => null)]);
    if (!result) {
      child.kill("SIGKILL");
      await exited;
    }
  }

  return {
    socketPath,
    statePath,
    stderr: () => stderr,
    listening: () => waitFor(() => fs.existsSync(socketPath)),
    stop
  };
}

async function connectClient(socketPath) {
  const socket = await new Promise((resolve, reject) => {
    const candidate = net.createConnection({ path: socketPath });
    candidate.on("connect", () => resolve(candidate));
    candidate.on("error", reject);
  });
  socket.setEncoding("utf8");

  let nextId = 1;
  let buffer = "";
  const pending = new Map();
  const notifications = [];
  const notificationWaiters = new Set();
  const closed = new Promise((resolve) => socket.on("close", resolve));

  function notifyWaiters(message) {
    for (const waiter of [...notificationWaiters]) {
      if (waiter.predicate(message)) {
        notificationWaiters.delete(waiter);
        waiter.resolve(message);
      }
    }
  }

  socket.on("data", (chunk) => {
    buffer += chunk;
    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = buffer.slice(0, newlineIndex);
      buffer = buffer.slice(newlineIndex + 1);
      newlineIndex = buffer.indexOf("\n");
      if (!line.trim()) {
        continue;
      }
      const message = JSON.parse(line);
      if (message.id !== undefined) {
        const request = pending.get(message.id);
        if (request) {
          pending.delete(message.id);
          if (message.error) {
            request.reject(new Error(message.error.message));
          } else {
            request.resolve(message.result);
          }
        }
        continue;
      }
      notifications.push(message);
      notifyWaiters(message);
    }
  });

  socket.on("error", (error) => {
    for (const request of pending.values()) {
      request.reject(error);
    }
    pending.clear();
  });

  function request(method, params = {}) {
    const id = nextId++;
    const response = new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
    });
    socket.write(`${JSON.stringify({ id, method, params })}\n`);
    return response;
  }

  async function waitForNotification(predicate, timeoutMs = 10000) {
    const existing = notifications.find(predicate);
    if (existing) {
      return existing;
    }
    const notification = new Promise((resolve) => {
      notificationWaiters.add({ predicate, resolve });
    });
    return Promise.race([notification, delay(timeoutMs).then(() => null)]);
  }

  await request("initialize", {});
  return {
    request,
    waitForNotification,
    async end() {
      socket.end();
      await closed;
    },
    destroy() {
      socket.destroy();
    }
  };
}

async function waitForUnsubscribes(statePath, expectedThreadIds) {
  const expected = [...expectedThreadIds].sort();
  const found = await waitFor(() => {
    const actual = [...(readState(statePath)?.unsubscribeRequests ?? [])].sort();
    return actual.length === expected.length && actual.every((threadId, index) => threadId === expected[index]);
  });
  assert.equal(found, true, `expected unsubscribe requests for ${expected.join(", ")}`);
}

test("broker unsubscribes a completed task thread when its client closes", async (t) => {
  const broker = startBroker();
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const started = await client.request("thread/start", { cwd: process.cwd(), ephemeral: true });
  const threadId = started.thread.id;
  await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "test normal completion" }]
  });
  const completed = await client.waitForNotification(
    (message) => message.method === "turn/completed" && message.params?.threadId === threadId
  );
  assert.ok(completed, "task never completed");

  await client.end();
  await waitForUnsubscribes(broker.statePath, [threadId]);
  assert.deepEqual(readState(broker.statePath).subscriptions, []);
});

test("broker keeps a resumed thread subscribed until its final client closes", async (t) => {
  const broker = startBroker();
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const firstClient = await connectClient(broker.socketPath);
  const threadId = (await firstClient.request("thread/start", { cwd: process.cwd(), ephemeral: false })).thread.id;
  const secondClient = await connectClient(broker.socketPath);
  await secondClient.request("thread/resume", { threadId });

  await firstClient.end();
  await delay(250);
  assert.deepEqual(readState(broker.statePath).unsubscribeRequests, []);

  await secondClient.end();
  await waitForUnsubscribes(broker.statePath, [threadId]);
});

test("broker unsubscribes source and detached review threads", async (t) => {
  const broker = startBroker();
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const sourceThreadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: true })).thread.id;
  const review = await client.request("review/start", {
    threadId: sourceThreadId,
    delivery: "detached",
    target: { type: "uncommittedChanges" }
  });
  assert.notEqual(review.reviewThreadId, sourceThreadId);
  const completed = await client.waitForNotification(
    (message) => message.method === "turn/completed" && message.params?.threadId === review.reviewThreadId
  );
  assert.ok(completed, "review never completed");

  await client.end();
  await waitForUnsubscribes(broker.statePath, [sourceThreadId, review.reviewThreadId]);
});

test("broker unsubscribes a forked thread when its client closes", async (t) => {
  const broker = startBroker();
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const sourceThreadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: false })).thread.id;
  const forkThreadId = (await client.request("thread/fork", { threadId: sourceThreadId, ephemeral: true })).thread.id;

  await client.end();
  await waitForUnsubscribes(broker.statePath, [sourceThreadId, forkThreadId]);
});

test("broker unsubscribes when a client disconnects during an active turn", async (t) => {
  const broker = startBroker("slow-task");
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const threadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: true })).thread.id;
  await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "disconnect this client" }]
  });
  client.destroy();

  await waitForUnsubscribes(broker.statePath, [threadId]);
});

test("broker unsubscribes auto-subscribed subagent threads", async (t) => {
  const broker = startBroker("with-subagent");
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const threadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: true })).thread.id;
  await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "delegate this task" }]
  });
  const completed = await client.waitForNotification(
    (message) => message.method === "turn/completed" && message.params?.threadId === threadId
  );
  assert.ok(completed, "task never completed");

  const subagentThread = readState(broker.statePath).threads.find((thread) => thread.name === "design-challenger");
  assert.ok(subagentThread, "subagent thread was not created");

  await client.end();
  await waitForUnsubscribes(broker.statePath, [threadId, subagentThread.id]);
  assert.deepEqual(readState(broker.statePath).subscriptions, []);
});

test("broker unsubscribes a child thread created after its client disconnects", async (t) => {
  const broker = startBroker("with-delayed-subagent");
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const threadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: true })).thread.id;
  await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "disconnect before delegation" }]
  });
  client.destroy();

  const childCreated = await waitFor(() =>
    readState(broker.statePath)?.threads.some((thread) => thread.name === "delayed-design-challenger")
  );
  assert.equal(childCreated, true, "delayed child thread was not created");
  const childThread = readState(broker.statePath).threads.find(
    (thread) => thread.name === "delayed-design-challenger"
  );

  await waitForUnsubscribes(broker.statePath, [threadId, childThread.id]);
  assert.deepEqual(readState(broker.statePath).subscriptions, []);
});

test("broker keeps shared upstream subscriptions when one client explicitly unsubscribes", async (t) => {
  const broker = startBroker();
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const firstClient = await connectClient(broker.socketPath);
  const threadId = (await firstClient.request("thread/start", { cwd: process.cwd(), ephemeral: false })).thread.id;
  const secondClient = await connectClient(broker.socketPath);
  await secondClient.request("thread/resume", { threadId });

  assert.deepEqual(await firstClient.request("thread/unsubscribe", { threadId }), { status: "unsubscribed" });
  assert.deepEqual(readState(broker.statePath).unsubscribeRequests, []);
  assert.deepEqual(readState(broker.statePath).subscriptions, [threadId]);

  const thirdClient = await connectClient(broker.socketPath);
  assert.deepEqual(await thirdClient.request("thread/unsubscribe", { threadId }), { status: "notSubscribed" });
  assert.deepEqual(readState(broker.statePath).unsubscribeRequests, []);

  await firstClient.end();
  await thirdClient.end();
  await secondClient.end();
  await waitForUnsubscribes(broker.statePath, [threadId]);
  assert.deepEqual(readState(broker.statePath).subscriptions, []);
});

test("broker does not reclaim ownership from notifications during explicit unsubscribe", async (t) => {
  const broker = startBroker("unsubscribe-notifies");
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const threadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: true })).thread.id;
  assert.deepEqual(await client.request("thread/unsubscribe", { threadId }), { status: "unsubscribed" });
  await client.end();
  await delay(250);

  assert.deepEqual(readState(broker.statePath).unsubscribeRequests, [threadId]);
  assert.deepEqual(readState(broker.statePath).subscriptions, []);
});

test("broker logs upstream unsubscribe failures", async (t) => {
  const broker = startBroker("unsubscribe-fails");
  t.after(() => broker.stop());
  assert.equal(await broker.listening(), true, `broker never listened: ${broker.stderr()}`);

  const client = await connectClient(broker.socketPath);
  const threadId = (await client.request("thread/start", { cwd: process.cwd(), ephemeral: true })).thread.id;
  await client.end();

  const requestObserved = await waitFor(() => readState(broker.statePath)?.unsubscribeRequests?.includes(threadId));
  assert.equal(requestObserved, true, "unsubscribe request was not observed");
  const warningObserved = await waitFor(
    () => broker.stderr().includes(`Failed to unsubscribe Codex thread ${threadId}: thread unsubscribe failed`)
  );
  assert.equal(warningObserved, true, "unsubscribe failure was not logged");
  assert.deepEqual(readState(broker.statePath).subscriptions, [threadId]);
});
