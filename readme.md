# Swytch

A Redis-compatible distributed cache. No leaders, no quorums, no clock sync. Drop it in, connect with `redis-cli`, and
go.

## The Short Version

Swytch is a Redis compatible server. You connect with any Redis client, you run your commands, everything works.
Strings, hashes, sets, sorted sets, streams, pub/sub, scripting, transactions, ACLs — 463 commands across all data
types.

The difference is underneath. Every mutation produces *effects*, not state changes. Effects encode what happened and
what they observed. The happened-before relation isn't inferred from clocks — it's the data structure itself.

That means replication without leaders, merges without coordination, and consistency guarantees that come from the math,
not the infrastructure.

## Quick Start

```bash
# Build
go build -o swytch .

# Run it
./swytch redis

# That's it. Connect with any Redis client.
redis-cli -p 6379
```

Embed it in your application process over a unix socket (preferred — no TCP overhead, no network attack surface):

```bash
./swytch redis --unixsocket /tmp/swytch.sock --bind ""
```

```python
import redis
r = redis.Redis(unix_socket_path="/tmp/swytch.sock")
r.set("hello", "world")
```

### More Options

```bash
# Custom port and memory limit
./swytch redis --port 6380 --maxmemory 256mb --bind 0.0.0.0

# TLS (client-facing)
./swytch redis --tls-cert-file server.crt --tls-key-file server.key

# mTLS (require client certs)
./swytch redis --tls-cert-file server.crt --tls-key-file server.key --tls-ca-cert-file ca.crt

# Metrics and tracing
./swytch redis --metrics-port 9090 --otel-endpoint localhost:4318
```

Run `./swytch redis -h` for the full list.

### Building

```bash
just build              # development
just build-release      # stripped, trimpath
just build-platform linux arm64
just test               # tests
just test-race          # tests with race detector
just proto              # regenerate protobuf definitions
```

## Regional Clustering

Spin up multiple nodes in the same region and they find each other. No leader election, no quorum configuration. Every
node accepts writes. Commutative operations (counter increments, set additions) merge automatically. Conflicting
transactions are resolved deterministically — every node independently picks the same winner using the same function on
the same inputs. No voting required.

### Getting Started

Generate a shared passphrase — this is the sole trust root for the cluster:

```bash
./swytch gen-passphrase
# outputs: YUa6WXJDsloKgx4BQWV2edOiH3U2Ym4O5VLR1jrVvO4
```

Point every node at a DNS name that resolves to the other nodes:

```bash
# Node 1
./swytch redis --cluster-passphrase "YUa6..." --join my-cache.local

# Node 2
./swytch redis --cluster-passphrase "YUa6..." --join my-cache.local
```

That's it. Nodes discover each other via DNS, form a cluster over QUIC+mTLS, and replicate automatically. No config
files, no static node lists, no coordination.

Each node derives a shared CA from the passphrase and generates ephemeral leaf certificates on startup. Nodes
authenticate each other via mTLS over QUIC — no manual certificate distribution required.

### DNS Compatibility

The `--join` flag accepts any DNS name. SRV records are tried first (host:port per entry), falling back to A/AAAA
records combined with `--cluster-port`.

| Environment               | What DNS returns               | How it works         |
|---------------------------|--------------------------------|----------------------|
| Consul                    | SRV with host:port per service | SRV                  |
| K8s headless service      | A record per pod IP            | A + cluster port     |
| K8s headless + named port | SRV with pod IP + port         | SRV                  |
| Docker Compose            | A record per container         | A + cluster port     |
| Manual `/etc/hosts`       | A record                       | A + cluster port     |

DNS is only used for bootstrap. Once joined, nodes discover peers through membership effects. DNS is re-resolved only
if a node loses all peers and needs to re-bootstrap.

### Docker Compose

```yaml
services:
  cache:
    image: swytch
    command: >
      swytch redis
        --bind 0.0.0.0
        --cluster-passphrase ${CLUSTER_PASSPHRASE}
        --join cache
    deploy:
      replicas: 3
```

### Kubernetes

```yaml
apiVersion: v1
kind: Service
metadata:
  name: swytch
spec:
  clusterIP: None  # headless
  selector:
    app: swytch
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: swytch
spec:
  serviceName: swytch
  replicas: 3
  template:
    spec:
      containers:
        - name: swytch
          args:
            - redis
            - --bind=0.0.0.0
            - --cluster-passphrase=$(CLUSTER_PASSPHRASE)
            - --join=swytch
```

### Membership

Membership is not a special subsystem — it uses the same effects engine as all other data. Each node periodically
writes a heartbeat effect on an internal key. Crashed nodes expire after 30 seconds. Graceful shutdowns remove the
entry immediately. You can inspect membership with any Redis client:

```bash
redis-cli HGETALL __swytch:members
```

## How It Works

If you just want to use it, you can stop reading here. The rest is for people who want to know why it works.

### The Causal Effect Log

In 1978, Lamport described two approaches to ordering events in distributed systems. Explicitly encode what each
operation observed, or use logical clocks to approximate that ordering. The field chose clocks. Forty-eight years of
increasingly sophisticated clocks — vector clocks, version vectors, hybrid logical clocks — all trying to infer
causality from timestamps.

Swytch chose the other path.

Every mutation produces effects. Each effect carries a key identifier, dependency references to prior effects it
observed, a semantic tag, and a node identifier. Effects are append-only and immutable. Each node mints addresses in its
own namespace — `(NodeID, Sequence)` — so writes never collide. No coordination required.

The dependency DAG *is* the ordering. If effect B depends on effect A, B happened after A. If two effects share the same
dependencies, they observed the same state. This isn't metadata bolted on after the fact. It's the structure itself.

### Deterministic Fork-Choice

When multiple transactions race against the same causal base, a deterministic rule picks the winner: each commit
computes `H = hash(NodeID || HLC)`, lowest H wins. Every node applies the same function to the same inputs, gets the
same result. Combined with a bounded *horizon wait* — like Spanner's commit-wait but using software clocks only — this
gives exactly-once transaction semantics without voting.

### Light Cone Consistency

From any node's perspective:

- **Read-your-writes.** Your effects are visible to you immediately.
- **Monotonic reads.** You never go backward in causal time.
- **Causal consistency.** If A caused B, and you see B, you've already seen A.
- **Bounded convergence.** Any effect at node X reaches node Y within `d(X,Y) + e`, where `d` is network delay.
- **Deterministic convergence.** All nodes observing the same tip set produce bit-identical state.

No unbounded "eventual" window. Convergence time is propagation delay. Full stop.

### Adaptive Serialization

Low contention: nodes race, fork-choice picks a winner, zero coordination cost. High contention on a hot key: after a
few failed commits, a node emits a serialization request. Contending nodes route through a dynamically-selected leader (
chosen to minimize worst-case latency to all contenders) until contention subsides. Then the leader dissolves.

This happens per-key, at runtime, automatically. One hot list serializes while millions of other keys stay fully
distributed.

### Safe and Holographic Modes

**Safe mode** (default): During a partition, commutative operations continue on both sides. Transactions block. On
reconnect, commutative effects auto-merge. No duplicate commits, no lost transactions.

**Holographic mode** (opt-in, per key prefix): Both sides of a partition keep accepting everything — including
transactions. Each partition becomes a complete, internally consistent database. On reconnect, one-sided commits merge
via fork-choice. Both-sided commits are flagged for application review. The causal DAG preserves both histories. Nothing
is silently discarded.

The choice is per key-range, changeable at runtime.

## How It Compares

| Property                  | Redis Cluster | Raft/Paxos   | CRDTs                       | Swytch               |
|---------------------------|---------------|--------------|-----------------------------|----------------------|
| Exactly-once transactions | No            | Yes          | No                          | Yes                  |
| No static leader          | No            | No           | Yes                         | Yes                  |
| No per-write coordination | No            | No           | Yes                         | Yes                  |
| Partition behavior        | Block         | Block        | Continue (commutative only) | Configurable per-key |
| Clock requirements        | None          | Synchronized | Vector/logical              | HLC (software only)  |

## Internals

### CloxCache (L0 — RAM)

Lock-free, sharded, adaptive in-memory cache. Items with access frequency above a learned threshold are protected from
eviction. The threshold adapts online per-shard — no manual tuning.

### Effects Engine

Causal effect log with DAG-based dependency tracking, horizon wait for transactional commits, and adaptive
serialization. Formally verified via TLA+ (`CausalEffectLog.tla`, `ExactlyOnce.tla`).

### Transport

QUIC (RFC 9000) for peer-to-peer communication. Bidirectional streams for effect replication. mTLS for peer
authentication, derived from a shared passphrase (HKDF-SHA256 + Ed25519).

### Observability

Prometheus metrics: connections, per-command counts, cache hits/misses/evictions, memory, latency percentiles (p50/p99),
adaptive thresholds, network I/O. OpenTelemetry tracing via OTLP HTTP with trace context injected into structured logs.

## Configuration Reference

### Server Flags

| Flag            | Default   | Description                             |
|-----------------|-----------|-----------------------------------------|
| `--port`        | 6379      | TCP port                                |
| `--bind`        | 127.0.0.1 | Bind address                            |
| `--maxmemory`   | 64mb      | Memory limit (b/kb/mb/gb/tb)            |
| `--databases`   | 16        | Number of databases                     |
| `--threads`     | 0         | Worker threads (0 = all CPUs)           |
| `--requirepass` |           | AUTH password (use `--aclfile` instead) |
| `--aclfile`     |           | ACL configuration file                  |
| `--maxclients`  | 0         | Max connections (0 = unlimited)         |
| `--timeout`     | 0         | Client idle timeout in seconds          |
| `--unixsocket`  |           | Unix domain socket path                 |
| `--debug`       | false     | Debug logging                           |
| `--log-format`  | text      | Log format: text or json                |

### Client TLS Flags

| Flag                 | Default | Description           |
|----------------------|---------|-----------------------|
| `--tls-cert-file`    |         | TLS certificate (PEM) |
| `--tls-key-file`     |         | TLS private key (PEM) |
| `--tls-ca-cert-file` |         | CA cert for mTLS      |
| `--tls-min-version`  | 1.2     | Minimum TLS version   |

### Cluster Flags

| Flag                    | Default      | Description                                        |
|-------------------------|--------------|----------------------------------------------------|
| `--cluster-passphrase`  | *(required)* | Shared passphrase for cluster mTLS                 |
| `--join`                |              | DNS name to resolve for peer discovery             |
| `--cluster-port`        | port + 1000  | QUIC port for cluster traffic                      |
| `--cluster-advertise`   | auto-detect  | Address:port this node advertises to peers         |

### Observability Flags

| Flag              | Default | Description                          |
|-------------------|---------|--------------------------------------|
| `--metrics-port`  | 0       | Prometheus metrics port              |
| `--otel-endpoint` |         | OTLP HTTP endpoint                   |
| `--otel-insecure` | false   | Use HTTP instead of HTTPS for OTLP   |
| `--pprof`         |         | pprof address (e.g., localhost:6060) |

### Subcommands

| Command                 | Description                        |
|-------------------------|------------------------------------|
| `swytch redis`          | Start a Redis-compatible server    |
| `swytch gen-passphrase` | Generate a cluster mTLS passphrase |
| `swytch version`        | Show version information           |
| `swytch help`           | Show help                          |

## Research

- **"Causal Effect Log and Exactly-Once Transactions"** — Landers & Kramer, 2026. The construction: addressable
  immutability, deterministic fork-choice, light cone consistency. Core safety properties verified via TLA+.

- **"Split Brain as the Inevitable Cost of Leaderless Transactions"** — Landers & Kramer, 2026. Proves that holographic
  divergence is unavoidable for any leaderless, available, quorum-free protocol with exactly-one winner selection.

## License

AGPL-3.0. See source file headers.
