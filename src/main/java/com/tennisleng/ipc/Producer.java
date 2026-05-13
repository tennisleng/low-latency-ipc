package com.tennisleng.ipc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Producer process — writes messages into the shared ring buffer.
 *
 * Usage:
 *   ./gradlew runProducer                         # default: 1ms throttle, YIELD wait
 *   ./gradlew runProducer --args="--fast"          # no throttle
 *   ./gradlew runProducer --args="--count 100000"  # send N messages then stop
 *
 * Run Consumer in another terminal:
 *   ./gradlew runConsumer
 *
 * They communicate through a memory-mapped file — no sockets, no serialization,
 * no kernel context switches. Just raw shared memory.
 */
public class Producer {

    private static final Path SHARED_FILE = Path.of("/tmp/ipc-ring-buffer.dat");
    private static final int CAPACITY = 1024;
    private static final int MAX_PAYLOAD = 256;

    public static void main(String[] args) throws IOException, InterruptedException {
        // ── Parse CLI args ──────────────────────────────────────────────
        boolean fast = false;
        long maxMessages = Long.MAX_VALUE; // infinite by default
        WaitStrategy waitStrategy = WaitStrategy.YIELD;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--fast" -> fast = true;
                case "--count" -> maxMessages = Long.parseLong(args[++i]);
                case "--spin" -> waitStrategy = WaitStrategy.SPIN;
                case "--sleep" -> waitStrategy = WaitStrategy.SLEEP;
            }
        }

        // ── Print config ────────────────────────────────────────────────
        System.out.println("┌──────────────────────────────────────────┐");
        System.out.println("│       PRODUCER — Low Latency IPC        │");
        System.out.println("├──────────────────────────────────────────┤");
        System.out.printf("│  File:     %s%n", SHARED_FILE);
        System.out.printf("│  Capacity: %d slots%n", CAPACITY);
        System.out.printf("│  Payload:  %d bytes max%n", MAX_PAYLOAD);
        System.out.printf("│  Mode:     %s%n", fast ? "FAST (no throttle)" : "THROTTLED (1ms)");
        System.out.printf("│  Wait:     %s%n", waitStrategy);
        System.out.printf("│  Messages: %s%n", maxMessages == Long.MAX_VALUE ? "infinite" : maxMessages);
        System.out.println("└──────────────────────────────────────────┘");
        System.out.println();

        // ── Latency tracking ────────────────────────────────────────────
        LatencyHistogram writeLatency = new LatencyHistogram();
        final WaitStrategy ws = waitStrategy;

        // ── Graceful shutdown on Ctrl+C ─────────────────────────────────
        final boolean[] running = {true};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running[0] = false;
            System.out.println("\n[Producer] Shutting down...");
            System.out.println(writeLatency.prettyPrint());
        }));

        try (SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD)) {
            ring.reset();

            long messageCount = 0;
            long lastReportTime = System.nanoTime();

            while (running[0] && messageCount < maxMessages) {
                // Build message (allocates — but outside the critical path)
                String message = "msg-" + messageCount;
                byte[] payload = message.getBytes(StandardCharsets.UTF_8);

                // ── Timed write ─────────────────────────────────────────
                long t0 = System.nanoTime();

                while (!ring.write(payload)) {
                    ws.idle();
                }
                ws.reset();

                writeLatency.recordSince(t0);

                // ── Periodic reporting ──────────────────────────────────
                messageCount++;
                long now = System.nanoTime();
                if (now - lastReportTime > 2_000_000_000L) { // every ~2 seconds
                    double elapsed = (now - lastReportTime) / 1_000_000_000.0;
                    System.out.printf("[Producer] %,d msgs sent (%.0f msg/s)  buffer≈%d/%d%n",
                            messageCount,
                            messageCount / elapsed,
                            ring.size(),
                            ring.getCapacity());
                    lastReportTime = now;
                }

                if (!fast) {
                    Thread.sleep(0, 100_000); // ~100µs throttle
                }
            }

            System.out.printf("%n[Producer] Finished. %,d messages sent.%n", messageCount);
            System.out.println(writeLatency.prettyPrint());
        }
    }
}
