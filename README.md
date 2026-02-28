# OMS — Ultra-Low Latency Equities Order Management System

A production-grade skeleton for a limit-order-book matching engine in Java 21.

## Architecture

```
┌──────────┐  TCP (binary)  ┌───────────┐  Aeron UDP  ┌──────────────────┐
│  UI/Tool │ ◄────────────► │  Gateway  │ ◄─────────► │  Matching Engine │
└──────────┘                └───────────┘             └──────────────────┘
                               Netty                    P=32 partitions
                            session mgmt              price-time priority
                            seq/idempotency           IOC + GTC
```

**Modules:**
- `common`   — Config, metrics (HdrHistogram), shared utilities
- `protocol` — Binary wire codec (flyweight over Agrona DirectBuffer, zero-alloc)
- `gateway`  — Netty TCP server + Aeron publisher/subscriber + session management
- `engine`   — Partitioned matching engine + limit order book
- `tools`    — Load generator + latency benchmark

## Message Flow

1. Client connects to Gateway over TCP
2. Client sends `LOGON` → Gateway assigns `sessionId`, returns `LOGON_ACK`
3. Client sends `NEW_ORDER` with monotonic `clientSeqNo`
4. Gateway validates seqNo, assigns `internalOrderId`, publishes to Aeron inbound stream
5. Engine partition receives order, acks, matches, publishes fills/rejects to Aeron outbound stream
6. Gateway subscriber polls all outbound streams, routes replies to correct client channel

## Aeron Topology

```
Inbound  streams: 1000..1031  (one per partition, gateway→engine)
Outbound streams: 2000..2031  (one per partition, engine→gateway)
partition = instrumentId % P  (default P=32)
```

## Requirements

- JDK 21
- Maven 3.8+
- Linux recommended (Aeron performs best on Linux; `/dev/shm` used for media driver)

## Build

```bash
mvn clean package -DskipTests
# or with tests:
mvn clean package
```

## Run

### Local Development (Gateway + Engine in one process)

```bash
java -jar gateway/target/gateway-fat.jar
```

Default config (`oms.yml`) sets `runEngine=true` — the gateway launches all 32 engine
partitions in-process over local UDP. This is the easiest way to develop and test.

### Separate Processes (Gateway-only mode)

1. On the engine host, edit `oms.yml`:
```yaml
runGateway: false
runEngine: true
aeronChannel: aeron:udp?endpoint=0.0.0.0:20121
```
```bash
java -jar engine/target/engine-fat.jar oms.yml
```

2. On the gateway host, edit `oms.yml`:
```yaml
runGateway: true
runEngine: false
aeronChannel: aeron:udp?endpoint=<engine-host-ip>:20121
```
```bash
java -jar gateway/target/gateway-fat.jar oms.yml
```

### Run the Load Generator

```bash
# Default: 10,000 orders/sec for 10 seconds
java -jar tools/target/loadgen-fat.jar

# Custom: 50,000 orders/sec for 30 seconds
java -jar tools/target/loadgen-fat.jar oms.yml 50000 30
```

### Run Unit Tests

```bash
mvn test -pl engine
```

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `aeronDir` | `/dev/shm/aeron-oms` | Aeron media driver directory |
| `aeronChannel` | `aeron:udp?endpoint=localhost:20121` | Aeron channel URI |
| `partitions` | `32` | Number of engine partitions |
| `inboundStreamBase` | `1000` | Gateway→Engine stream IDs start here |
| `outboundStreamBase` | `2000` | Engine→Gateway stream IDs start here |
| `gatewayPort` | `7001` | TCP port for client connections |
| `gatewayBackpressureQueueLimit` | `4096` | Per-partition queue depth before SYSTEM_BUSY |
| `runGateway` | `true` | Start TCP gateway in this process |
| `runEngine` | `true` | Start engine partitions in this process |
| `metricsIntervalSecs` | `5` | Latency stats logging interval |

## Wire Protocol

All fields little-endian.

**TCP Frame:**
```
[2 bytes: total frame length][1 byte: MsgType][N bytes: payload]
```

**Key Message Types:**

| Code | Name | Direction |
|------|------|-----------|
| 1 | LOGON | Client→Gateway |
| 2 | NEW_ORDER | Client→Gateway |
| 3 | CANCEL_REQUEST | Client→Gateway |
| 20 | LOGON_ACK | Gateway→Client |
| 21 | ACK | Gateway→Client |
| 22 | REJECT | Gateway→Client |
| 23 | FILL | Gateway→Client |
| 24 | CANCEL_ACK | Gateway→Client |

**Prices:** stored as `int64 = price × 1,000,000` (6 decimal places).

## Performance Design

- **Zero allocation in hot paths**: Agrona `UnsafeBuffer`, `Long2ObjectOpenHashMap` (fastutil), pooled `Order` and `PriceLevel` objects
- **Single-threaded partition ownership**: no locks in matching path; one thread per partition
- **Price-time priority**: `TreeMap<Long, PriceLevel>` + intrusive doubly-linked lists within levels
- **FIFO within price level**: intrusive linked list, O(1) head removal
- **O(1) cancel**: `Long2ObjectOpenHashMap<Order>` per book
- **Backpressure**: Aeron back-pressure detected and propagated as `SYSTEM_BUSY` reject
- **HdrHistogram latency tracking**: ingress→ack, ingress→fill with p50/p99/p999/max

## Adding Persistence / Snapshotting

The engine partition is structured so that Aeron Archive can be added:
1. Subscribe via `AeronArchive.startRecording()` instead of a plain subscription
2. Add snapshot support by serializing `LimitOrderBook` state at regular intervals
3. Replay from the archive on startup before accepting new orders

## TODO / Phase 2

- [ ] Replace `TreeMap` with a custom lock-free skip-list or cache-friendly sorted array for ultra-hot paths
- [ ] Thread pinning via `affinity` library (JNA-based CPU affinity)
- [ ] Aeron Archive integration for persistence and replay
- [ ] Market orders
- [ ] Order replace (modify)
- [ ] Admin CLI (`tools` module) for book inspection and stats
- [ ] HTTP `/metrics` endpoint (Prometheus format)
- [ ] Integration test with embedded engine + gateway + load generator
