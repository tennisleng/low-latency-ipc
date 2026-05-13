# low-latency-ipc

I'm building a lock-free IPC system in Java from scratch. Two JVM processes talking through shared memory — no sockets, no serialization, no garbage collection in the hot path.

The idea comes from the LMAX Disruptor, which is what a lot of trading firms use under the hood. I wanted to actually understand how it works instead of just importing a library.

## what this is

A single-producer, single-consumer (SPSC) ring buffer backed by a memory-mapped file. Process A writes messages into a file. Process B reads them out. Except it's not really "file I/O" — the OS maps the same physical memory page into both processes, so the data never actually gets copied anywhere. It's basically `mmap` but in Java.

The tricky part isn't the ring buffer logic (that's just LeetCode 622). The tricky part is making sure the consumer doesn't read a half-written message. That's where `VarHandle` comes in — it lets you do acquire/release memory barriers like `std::memory_order_acquire` in C++, but without paying for a full `volatile` fence.

```
Producer (JVM 1)                          Consumer (JVM 2)
      │                                        │
      │    ┌────────────────────────────┐       │
      ├───>│  memory-mapped file        │<──────┤
      │    │  /tmp/ipc-ring-buffer.dat  │       │
      │    └────────────────────────────┘       │
      │                                        │
 setRelease()                            getAcquire()
```

## why not just use sockets / a blocking queue

- **Sockets** go through the kernel. Every send/recv is a context switch + a memcpy. That's fine for most things, but it's not sub-microsecond.
- **`LinkedBlockingQueue`** allocates a new node object for every message. Eventually the GC has to clean all of them up, and when it does, your latency spikes from ~200ns to 10ms+.

This avoids both problems. The data lives off-heap in a memory-mapped region, and the consumer reuses a single `MessageView` object (flyweight pattern) so there's literally zero allocation in the hot loop.

## project layout

```
src/main/java/com/tennisleng/ipc/
├── SPSCRingBuffer.java    -- the actual ring buffer. heavily commented.
├── MessageView.java       -- flyweight view over a message slot (no copying)
├── Producer.java          -- writer process
├── Consumer.java          -- reader process
└── demo/
    └── HelloMmap.java     -- bare-bones mmap example to start with

src/test/java/             -- junit tests
src/jmh/java/              -- benchmark: SPSC vs LinkedBlockingQueue
```

## running it

You'll need Java 21+ and the Gradle wrapper handles the rest.

```bash
# start with this — just writes a long to a file and reads it back via mmap
./gradlew runHelloMmap

# then try the full thing (two separate terminals)
./gradlew runProducer
./gradlew runConsumer

# benchmarks
./gradlew jmh

# tests
./gradlew test
```

## how I'm learning this

I'm following a ~4 week plan. Each week builds on the last.

**Week 1** — just reading. Cache lines, false sharing, the Java Memory Model. Understanding *why* you pad the producer and consumer sequence counters to sit on different 64-byte cache lines.

**Week 2** — the `HelloMmap` demo. Getting comfortable with `FileChannel.map()` and `MappedByteBuffer`. It's surprisingly straightforward if you've used `mmap` in C.

**Week 3** — building the actual SPSC queue. The ring buffer indexing is easy (`seq & (capacity - 1)` instead of modulo). The hard part is getting the `VarHandle` acquire/release semantics right so the consumer never sees a torn write.

**Week 4** — the flyweight pattern + benchmarking with JMH. The goal is to prove that latency stays flat while a standard `LinkedBlockingQueue` gets destroyed by GC pauses.

## notes to self

- Capacity has to be a power of 2 (bitmask trick for wrapping)
- The commit flag uses release semantics on write, acquire on read — this is what guarantees the payload is fully written before the consumer touches it
- `Thread.onSpinWait()` is the Java version of `_mm_pause()` — tells the CPU you're in a spin loop so it can save power
- The 56 bytes of padding between the producer and consumer sequence pointers isn't wasted space — it prevents false sharing. Without it, the two CPU cores fight over the same cache line and throughput drops ~10x.

## license

MIT
