# Blog_Using_KV_Store
Build a key value store that supports Set, Get, Delete, Bulk Set.
# Distributed Key-Value Store

A high-performance, distributed key-value database built from scratch with TCP/IP networking, featuring ACID compliance, replication, leader election, and advanced indexing capabilities.

## 📋 Project Overview

This project implements a production-grade distributed database system inspired by Cassandra and other modern NoSQL databases. It was built incrementally through multiple iterations, each adding more sophisticated features.

## 🚀 Key Features

### Core Database Features
- **TCP-based Protocol**: Custom binary protocol with length-prefixed JSON messages
- **Persistent Storage**: Write-Ahead Logging (WAL) with `fsync()` for 100% durability
- **ACID Compliance**: Full transaction support with atomicity guarantees
- **High Performance**: Optimized for write throughput with batch operations

### Distributed Systems Features
- **3-Node Cluster**: Primary-secondary replication architecture
- **Leader Election**: Raft-inspired consensus protocol for automatic failover
- **Masterless Mode**: Optional quorum-based replication (Cassandra-style)
- **Fault Tolerance**: Survives node failures with automatic recovery

### Advanced Indexing
- **Inverted Index**: Full-text search capabilities with tokenization
- **Vector Embeddings**: Similarity search using cosine similarity
- **Index Synchronization**: Automatic index replication across cluster

## 📊 Development Timeline

### Phase 1: Foundation (Single-Node KV Store)
- Basic TCP server/client implementation
- Set, Get, Delete, BulkSet operations
- WAL-based persistence
- Graceful shutdown and recovery

### Phase 2: Reliability & Testing
- ACID compliance testing
- Concurrent operation testing
- Write throughput benchmarks
- Durability testing with simulated crashes
- Debug mode for power outage simulation

### Phase 3: Distributed Architecture
- Multi-node cluster setup
- Asynchronous replication
- Heartbeat monitoring
- Leader election protocol
- Automatic failover handling

### Phase 4: Advanced Features
- Inverted index for full-text search
- Vector embeddings for similarity search
- Masterless replication with quorum
- Cross-platform compatibility (Windows/Linux)

## 🏗️ Architecture

```
┌─────────────────────────────────────────────┐
│           Client Applications               │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│         ClusterClient (Auto-failover)       │
└─────────────┬───────────────────────────────┘
              │
    ┌─────────┴─────────┬─────────────┐
    ▼                   ▼             ▼
┌─────────┐       ┌─────────┐   ┌─────────┐
│ Node 0  │◄─────►│ Node 1  │◄─►│ Node 2  │
│(Primary)│       │(Secondary)  │(Secondary)
└────┬────┘       └────┬────┘   └────┬────┘
     │                 │             │
     ▼                 ▼             ▼
  [WAL+Store]      [WAL+Store]   [WAL+Store]
  [Indexes]        [Indexes]     [Indexes]
```

## 📈 Performance Characteristics

- **Write Throughput**: ~1,000-5,000 writes/sec (depending on data size)
- **Durability**: 100% (zero acknowledged writes lost after crashes)
- **Replication**: Asynchronous (eventual consistency)
- **Election Time**: 5-10 seconds after primary failure
- **Quorum Latency**: ~2-5ms for 3-node local cluster

## 🧪 Testing Coverage

### Functional Tests
- ✅ Set/Get/Delete operations
- ✅ Bulk operations atomicity
- ✅ Persistence across restarts
- ✅ Concurrent write isolation

### Cluster Tests
- ✅ Primary-secondary replication
- ✅ Leader election on failure
- ✅ Automatic client failover
- ✅ Index synchronization

### Index Tests
- ✅ Full-text search accuracy
- ✅ Similarity search ranking
- ✅ Multi-term queries

### Masterless Tests
- ✅ Quorum write acknowledgment
- ✅ Quorum read consistency
- ✅ Fault tolerance (1 node down)

## 🛠️ Technology Stack

- **Language**: Python 3.7+
- **Networking**: Raw TCP sockets
- **Serialization**: JSON
- **Persistence**: File-based WAL + Snapshots
- **Concurrency**: Threading with locks
- **Process Management**: Subprocess for testing

## 📦 Installation & Usage

```bash
# Clone the repository
git clone <repository-url>
cd distributed-kv-store

# Run all tests
python kv_store.py

# Or import and use programmatically
from kv_store import ClusterNode, ClusterClient

# Start a 3-node cluster
# (See detailed documentation for full setup)
```

## 📚 Detailed Documentation

For comprehensive documentation including:
- Detailed architecture diagrams
- Protocol specifications
- Implementation deep-dives
- Performance tuning guides
- Advanced usage examples

**See the [research branch README](../../tree/research/README.md)**

## 🔍 Project Structure

```
.
├── kv_store.py           # Main implementation (all-in-one)
├── README.md             # This file
└── [node_data_*]/        # Generated: Node storage directories
```

## 🎯 Use Cases

This database is suitable for:
- Learning distributed systems concepts
- Academic research in database internals
- Prototyping distributed applications
- Understanding replication and consensus
- Benchmarking custom storage engines

## ⚠️ Production Readiness

This is an **educational implementation** demonstrating distributed database concepts. While it includes production-quality features like:
- WAL-based durability
- ACID compliance
- Leader election
- Replication

It should **not** be used in production without:
- Security hardening (authentication, encryption)
- Monitoring and observability
- Advanced conflict resolution
- Optimized storage engine
- Network partition handling
- Comprehensive error recovery

## 🤝 Contributing

This project was built as a learning exercise. Contributions that improve educational value are welcome:
- Enhanced documentation
- Additional test cases
- Performance optimizations
- Bug fixes

## 📄 License

MIT License - See LICENSE file for details

## 🙏 Acknowledgments

Inspired by:
- Apache Cassandra (masterless architecture)
- Raft Consensus Algorithm (leader election)
- PostgreSQL (WAL design)
- Redis (simple protocol design)

---

**Built with ❤️ for learning distributed systems**