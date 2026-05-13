# Low-Latency IPC — SPSC Ring Buffer over Memory-Mapped Files

A zero-garbage, sub-microsecond inter-process communication system in Java, built from scratch using `VarHandle` memory barriers and `MappedByteBuffer` — no sockets, no serialization, no GC pauses.

Inspired by the **LMAX Disruptor** architecture used in real trading systems.

## Architecture

```
┌──────────────┐     Memory-Mapped File      ┌──────────────┐
│   Producer   │ ──── /tmp/ipc-ring.dat ───── │   Consumer   │
│   (JVM 1)    │     [shared memory region]   │   (JVM 2)    │
└──────────────┘                              └──────────────┘
         │                                            │
         ▼                                            ▼
   VarHandle.setRelease()                  VarHandle.getAcquire()
   (RELEASE barrier)                       (ACQUIRE barrier)
```

## Project Structure

```
src/
├── main/java/com/tennisleng/ipc/
│   ├── SPSCRingBuffer.java     ← Core ring buffer with VarHandle barriers
│   ├── MessageView.java        ← Flyweight (zero-alloc message reader)
│   ├── Producer.java           ← Producer process entry point
│   ├── Consumer.java           ← Consumer process entry point
│   └── demo/
│       └── HelloMmap.java      ← Week 2: basic mmap hello world
├── test/java/...
│   └── SPSCRingBufferTest.java ← Unit tests
└── jmh/java/...
    └── SPSCBenchmark.java      ← JMH: SPSC vs LinkedBlockingQueue
```

## Quick Start

```bash
# Week 2: Hello World with memory-mapped files
./gradlew runHelloMmap

# Week 3-4: Full IPC demo (run in separate terminals)
./gradlew runProducer
./gradlew runConsumer

# Week 4: Run benchmarks
./gradlew jmh

# Run tests
./gradlew test
```

## Learning Timeline

| Week | Focus | Key File |
|------|-------|----------|
| 1 | Cache lines, false sharing, JMM | Read the comments in `SPSCRingBuffer.java` |
| 2 | `FileChannel.map()`, `MappedByteBuffer` | `HelloMmap.java` |
| 3 | SPSC ring buffer + `VarHandle` barriers | `SPSCRingBuffer.java` |
| 4 | Flyweight pattern + JMH benchmarks | `MessageView.java` + `SPSCBenchmark.java` |

## Key Concepts

### Why Not Sockets?
Sockets involve kernel context switches and data copying. Memory-mapped files let two processes share the same physical memory page — zero copies, zero syscalls on the hot path.

### Why Not `volatile`?
`volatile` is a full memory fence (`std::memory_order_seq_cst`). For SPSC, we only need acquire/release semantics — half the cost. `VarHandle` gives us that control.

### What's the Flyweight Pattern?
Instead of `new Message()` on every read (which creates garbage for the GC), we reuse a single `MessageView` object that just points to different offsets in the buffer. Zero allocations = zero GC pauses.

## License

MIT
