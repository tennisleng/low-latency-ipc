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


## license

MIT
