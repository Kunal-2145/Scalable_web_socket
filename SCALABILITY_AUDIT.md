# Scalability Audit — `Scalable_web_socket`

**Branch:** `arena/c653f225-scalable-web-socket` · **Commit:** `fa21895` · **Reviewed:** 2026-11-10
**Stack:** Node 22 · Express 5 · `ws` 8.22 · ioredis 6 · Redis pub/sub

---

## Verdict

The **architecture is the right shape** — a fan-out WebSocket tier with Redis pub/sub as the
inter-instance bus is exactly how you make WebSockets horizontally scalable. A client can land on
any instance and still see every message. That is the hard part, and you got it right.

The problem is that the implementation has **no load-shedding of any kind**. It works beautifully
at 10 users and fails non-gracefully at 10,000. Three things will bite you first, in this order:

1. **A single slow client can OOM the process** — no backpressure check on `send()`.
2. **Global broadcast with no rooms** — you pay O(N²) delivery cost for a workload that needs O(N × room).
3. **No rate limit or auth** — one client can force `N` deliveries per message, an amplification vector.

Connection *memory* is fine, and fan-out *throughput* is decent. The ceiling is the single-threaded
event loop doing O(N) synchronous `send()` calls per message.

---

## What I measured

I replicated `server.ts`'s exact fan-out path (same `ws` version, same `forEach` + `readyState`
guard, Redis callback replaced by an HTTP trigger) and drove it with real sockets on a 2 vCPU /
3.9 GB box.

| Metric | Measured |
|---|---|
| RSS per idle connection | **~4.6 KB** (800 conns: 71.5 → 75.1 MB) |
| Handshake throughput | **~2,100–3,500 connects/sec** per instance |
| Fan-out loop cost | **~7.8 µs per client per broadcast** (7.03 ms @ 900 clients, 512 B) |
| End-to-end delivery | **~45,000 deliveries/sec**, **~23 MB/s** egress @ 900 clients¹ |
| Slow-consumer buffer growth | **7.87 MB, unbounded, never drains** (40 × 256 KiB msgs) |
| `console.log` cost | 2.2 µs per message in the hot path |
| Default `maxPayload` | **100 MiB** — a single frame can allocate 100 MB |

¹ The load generator was co-located on the same 2 vCPUs, so the *client* process was the bottleneck
here, not the server. Real per-core server headroom is higher; the 7.8 µs fan-out constant is the
honest one to size against.

### Capacity model

From the 7.8 µs constant, one instance saturates one core at roughly
**128,000 client-sends/second**:

```
max_broadcasts_per_sec ≈ 128,000 / local_clients
```

| Local clients | Event-loop time per broadcast | Max broadcasts/sec per core (100% busy) |
|---|---|---|
| 1,000 | 7.8 ms | ~128 |
| 5,000 | 39 ms | ~26 |
| 10,000 | 78 ms | ~13 |
| 25,000 | 195 ms | ~5 |

### The O(N²) wall

For a global chat with 10,000 users each sending one message every 10 seconds:

```
1,000 messages/sec  ×  10,000 recipients  =  10,000,000 deliveries/sec required
10,000,000 / 128,000 per core             =  ~78 cores
```

Now split the same 10,000 users into 100 rooms of 100:

```
1,000 messages/sec  ×  100 recipients  =  100,000 deliveries/sec  =  ~1 core
```

**Moving from global broadcast to room-scoped fan-out is a ~100× capacity win — bigger than any
other optimization available to you.** That is the single most valuable change on this list.

---

## Findings

### 🔴 Critical

**1. No backpressure → one slow client can OOM the process** — `server.ts:28-34`

```ts
c.send(message.toString())   // return value ignored, bufferedAmount never checked
```

I verified this: a client that completes the handshake and then stops reading accumulates
`bufferedAmount` linearly and **it never drains**:

```
after   5 msgs of 256KiB: bufferedAmount=0.00MB   rss=97.7MB
after  20 msgs of 256KiB: bufferedAmount=2.62MB   rss=100.2MB
after  40 msgs of 256KiB: bufferedAmount=7.87MB   rss=105.6MB
--- 3s idle later ---
{ "bufferedTotal": 7865040 }   ← still pinned, never freed
```

`ws` queues writes in userspace with no cap. A phone on a dying network, a backgrounded mobile tab,
a suspended VM — any of these pins memory forever. Because this is a *broadcast*, one stalled socket
combines with message rate to grow without bound until the process dies, taking every connection
with it. Fix this first.

**2. Global broadcast, no rooms or channels** — `server.ts:10,26-35`

Every message goes to every client on every instance. There is no way to say "send this to room X."
This is the O(N²) wall above. It also means each instance subscribes to one channel and receives
every message even when it holds zero interested clients, multiplying Redis outbound traffic by the
instance count.

**3. No authentication, rate limiting, or origin check** — `server.ts:14,38-55`

`new WebSocketServer({server})` accepts any origin, any client, unlimited messages. Combined with
broadcast, that is an amplifier: **1 inbound frame → N outbound deliveries.** One client sending
1,000 msg/s against 10,000 connected clients demands 10M deliveries/s. `verifyClient` also matters
for **Cross-Site WebSocket Hijacking** — without it, any website a logged-in user visits can open a
socket to your server.

**4. `maxPayload` left at the 100 MiB default** — `server.ts:14`

Verified in `node_modules/ws/lib/websocket-server.js:74`: `maxPayload: 100 * 1024 * 1024`. A single
client can make your process allocate 100 MB per frame. For a chat payload, 16–64 KB is generous.

### 🟠 High

**5. No heartbeat — dead sockets leak forever** — `server.ts:38`
The server never sends `ping`. Note `autoPong: true` means it answers *client* pings, but it cannot
detect a peer that silently vanished (mobile network handoff, NAT table expiry, laptop lid). Those
sockets sit in `wss.clients` in `OPEN` state until TCP keepalive gives up (~2 hours, often never),
consuming memory and fan-out work with no recipient.

**6. Crash-on-error and no graceful shutdown** — `server.ts:58-61`
`process.exit(1)` on any server error drops every connection at once. There is no `SIGTERM` handler
and no `wss.close()`, so every deploy severs all clients abruptly with no `1001 Going Away` frame.
Clients then reconnect in a synchronized stampede — a worse outage than the deploy itself.

**7. Redis is a single unhardened point of failure** — `docker-compose.yml`
```yaml
image: redis:latest        # unpinned — a rebuild can silently change major version
ports: ["6379:6379"]       # published on 0.0.0.0, no password, no TLS
```
No `requirepass`, no persistence, no `maxmemory` policy, no replica, no Sentinel/Cluster. Every
message on your platform flows through this one container. There is also no healthcheck and no app
service in the compose file — scaling is the manual `start:2` script.

**8. Redis pub/sub is fire-and-forget — messages are lost, never replayed**
An instance that is reconnecting, restarting, or briefly stalled misses everything with no error.
A client that reconnects gets no backlog and silently loses the gap. There are no message IDs,
timestamps, or sequence numbers, so you cannot detect or dedupe either. Redis Streams
(`XADD`/`XREADGROUP`) gives you durability, replay, consumer groups, and at-least-once delivery.

**9. Single process — one core of a multi-core box** — `package.json`
No `cluster`, no `worker_threads`. Since the bus already makes instances stateless, running one
worker per core behind `SO_REUSEPORT` (or `cluster`) is nearly free capacity.

**10. No observability or health endpoint**
No `/healthz`, no metrics, no structured logs. You cannot autoscale or even alert on a system whose
only signal is `console.log("Client connected")`. Track: connection count, messages/sec,
`bufferedAmount` p50/p99, event-loop lag, Redis publish latency, dropped-send count.

### 🟡 Medium

**11. Subscribe failure is never retried** — `server.ts:23-25`
```ts
redisSubscribe.subscribe(REDIS_CHANNEL).catch(err => console.log("Failed to subscribe to redis:", err.message))
```
ioredis queues commands while offline (`enableOfflineQueue` default `true`) and `retryStrategy`
backs off to a 5 s cap, so a brief Redis outage self-heals. But `maxRetriesPerRequest` defaults to
**20** (verified: `ioredis/built/redis/RedisOptions.js:57`). If Redis is unreachable for roughly a
minute at boot, the queued `SUBSCRIBE` is rejected and this `.catch` logs it — **and it is never
retried for the life of the process.** That instance stays alive, keeps accepting clients, and
silently never receives another bus message. Re-subscribe in the `ready`/`reconnecting` handlers.

**12. `console.log` on every message** — `server.ts:39,46,48`
2.2 µs each here, worse when Docker's log driver is slow (writes queue in memory). It also logs full
payloads — a PII issue and a log-flooding vector.

**13. Client never reconnects** — `public/server.html:36-39`
Status text says "refresh to reconnect." Users must manually refresh after any hiccup. When you do
add reconnection, use exponential backoff **with jitter**, or every client returns at once.

**14. Unbounded DOM growth** — `public/server.html:54-56`
Every message appends an `<li>` that is never removed. At 100 msg/s the tab accumulates 360k nodes
per hour and freezes — the browser becomes the bottleneck before the server does.

**15. No connection caps** — no global `maxConnections`, no per-IP limit. A single host can open
connections until the file descriptor limit (1024 in this sandbox) is hit, then every new client
fails. Production needs `LimitNOFILE=65535+` and a raised `net.core.somaxconn`; neither is documented.

### 🟢 Low

- Sender receives its own message (echo) — confusing UX for chat (`server.ts:28`).
- `data.toString()` silently mangles binary frames (`server.ts:45`).
- `ws.on("error")` logs but never closes the socket (`server.ts:41-43`).
- No tests — `npm test` exits 1.
- Message ordering is only FIFO per publisher connection, not global.

---

## What you got right (keep these)

- **Redis pub/sub bus for cross-instance fan-out** — correct pattern, avoids sticky sessions entirely.
- **Two separate connections for publish and subscribe** (`connection.ts:6-14`) — required; a subscribed connection can't issue normal commands.
- **`readyState === OPEN` guard before `send()`** (`server.ts:30`) — prevents the obvious throw.
- **`error` listeners on both Redis clients** (`connection.ts:26-32`) — avoids the classic ioredis unhandled-error crash on startup without Redis.
- **`perMessageDeflate: false`** (the default you kept) — the right CPU trade-off for small chat frames.
- **Client derives host/protocol from `window.location`** (`server.html:28-30`) — works behind a load balancer and from other devices.
- **HTTP and WebSocket on one port** — simplest possible LB configuration.

---

## Priority roadmap

### Phase 0 — stop the bleeding (~half a day)

```ts
// 1. Bounded sockets: cap payload and refuse to buffer for stalled peers
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024, perMessageDeflate: false });
const MAX_BUFFERED = 1 << 20;          // 1 MiB per socket

function broadcast(payload: string) {
  for (const c of wss.clients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    if (c.bufferedAmount > MAX_BUFFERED) {   // slow consumer -> shed load
      metrics.dropped++;
      continue;                              // or c.terminate() after N drops
    }
    c.send(payload);
  }
}

// 2. Heartbeat: reap silently-dead peers (measured above)
const alive = new WeakMap<WebSocket, boolean>();
wss.on("connection", (ws) => {
  alive.set(ws, true);
  ws.on("pong", () => alive.set(ws, true));
});
setInterval(() => {
  for (const ws of wss.clients) {
    if (!alive.get(ws)) { ws.terminate(); continue; }   // missed our ping
    alive.set(ws, false);
    ws.ping();
  }
}, 30_000).unref();

// 3. Graceful shutdown: no thundering herd on deploy
process.on("SIGTERM", () => {
  server.close();
  for (const c of wss.clients) c.close(1001, "going away");
  setTimeout(() => process.exit(0), 10_000).unref();
});
// ...and replace process.exit(1) in server.on("error") with a log + alert
```

Plus: per-connection token-bucket rate limit (e.g. 20 msg/s burst 40); `verifyClient` origin
allowlist; re-subscribe on Redis `ready`; drop the per-message `console.log` to debug level;
client reconnect with `delay = min(30s, 500ms * 2^attempt) * (0.5 + random())`; cap the DOM at
~500 nodes.

### Phase 1 — make it observable (~2–3 days)
`/healthz` (checks Redis ping) and `/metrics` (Prometheus): connections, msgs in/out, dropped sends,
`bufferedAmount` histogram, event-loop lag (`perf_hooks.monitorEventLoopDelay`), Redis latency.
Autoscale on connections/CPU, not memory.

### Phase 2 — remove the O(N²) wall (~1 week) ⭐ highest capacity ROI
Introduce **rooms with per-room channels**:

- `room:{id}` Redis channels instead of one `ws-message` channel.
- Each instance refcounts which rooms it has local members in and **`SUBSCRIBE`s/`UNSUBSCRIBE`s on demand** — so an instance only receives traffic it can deliver.
- Redis fan-in drops from `M × all_messages` to `M × only_relevant_messages`.
- Client state becomes `Map<roomId, Set<WebSocket>>` instead of iterating every client.

For ~100× more delivery capacity, this is the change that matters.

### Phase 3 — durability and sharding
Replace pub/sub with **Redis Streams** (or NATS / Kafka) for replay, backlog on reconnect,
at-least-once delivery, and message IDs. Put a **load balancer with WebSocket support** in front
(nginx `proxy_read_timeout 3600s`, or an L4 TCP LB), and run **`cluster`** so you use every core.
Then shard Redis by channel prefix (Redis Cluster) once a single instance's outbound bandwidth
becomes the ceiling — that happens when `instances × messages/sec × message_size` approaches
~100 MB/s.

---

## Bottom line

You built a correct **architecture** with an **unprotected implementation**. The bus-based design
will genuinely scale horizontally — no sticky sessions, no shared state in the app tier. But today a
single stalled mobile client can take down the whole fleet, and global broadcast means you burn ~100×
more capacity than a room-based design needs.

Ship **Phase 0** (backpressure, heartbeat, graceful shutdown, rate limit, `maxPayload`) before you
take real traffic — those are the differences between a demo and a service. Then do **Phase 2** before
you buy more servers: fixing the fan-out model is worth more than another 50 instances of the current one.
