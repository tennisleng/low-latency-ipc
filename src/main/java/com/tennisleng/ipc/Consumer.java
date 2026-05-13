package com.tennisleng.ipc;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Consumer process — reads messages from the shared ring buffer.
 *
 * Usage:
 *   ./gradlew runConsumer                    # default: YIELD wait
 *   ./gradlew runConsumer --args="--spin"    # busy-spin (lowest latency)
 *   ./gradlew runConsumer --args="--verbose" # print every message
 *
 * The key thing to look at: zero `new` keywords inside the hot loop.
 * The MessageView is allocated ONCE before the loop — that's the Flyweight pattern.
 * The LatencyHistogram records directly into pre-allocated arrays.
 */
public class Consumer {

    private static final Path SHARED_FILE = Path.of("/tmp/ipc-ring-buffer.dat");
    private static final int CAPACITY = 1024;
    private static final int MAX_PAYLOAD = 256;

    public static void main(String[] args) throws IOException {
        // ── Parse CLI args ──────────────────────────────────────────────
        boolean verbose = false;
        WaitStrategy waitStrategy = WaitStrategy.YIELD;

        for (String arg : args) {
            switch (arg) {
                case "--verbose" -> verbose = true;
                case "--spin" -> waitStrategy = WaitStrategy.SPIN;
                case "--sleep" -> waitStrategy = WaitStrategy.SLEEP;
                case "--progressive" -> waitStrategy = WaitStrategy.PROGRESSIVE;
            }
        }

        System.out.println("┌──────────────────────────────────────────┐");
        System.out.println("│       CONSUMER — Low Latency IPC        │");
        System.out.println("├──────────────────────────────────────────┤");
        System.out.printf("│  Wait: %s%n", waitStrategy);
        System.out.println("│  Waiting for messages...                │");
        System.out.println("└──────────────────────────────────────────┘");
        System.out.println();

        // ── Latency tracking ────────────────────────────────────────────
        LatencyHistogram readLatency = new LatencyHistogram();
        final WaitStrategy ws = waitStrategy;
        final boolean verb = verbose;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Consumer] Shutting down...");
            System.out.println(readLatency.prettyPrint());
        }));

        try (SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD)) {
            // ┌─────────────────────────────────────────────────────┐
            // │  Allocate ONCE — reuse forever. This is the key.    │
            // └─────────────────────────────────────────────────────┘
            MessageView view = new MessageView();
            byte[] reusableBuffer = new byte[MAX_PAYLOAD];

            long messageCount = 0;
            long lastReportTime = System.nanoTime();
            long reportInterval = 2_000_000_000L; // 2 seconds

            // ─── THE HOT LOOP ─── (zero allocations inside!) ────────
            while (true) {
                long t0 = System.nanoTime();

                if (ring.read(view)) {
                    readLatency.recordSince(t0);
                    ws.reset();

                    if (verb) {
                        // This allocates (String creation) — only use for debugging
                        System.out.println("[Consumer] " + view.getPayloadAsString());
                    }

                    messageCount++;

                    // Periodic reporting (uses nanoTime comparison — no allocation)
                    long now = System.nanoTime();
                    if (now - lastReportTime > reportInterval) {
                        System.out.printf("[Consumer] %,d msgs received  buffer≈%d/%d%n",
                                messageCount, ring.size(), ring.getCapacity());
                        lastReportTime = now;
                    }
                } else {
                    ws.idle();
                }
            }
        }
    }
}
