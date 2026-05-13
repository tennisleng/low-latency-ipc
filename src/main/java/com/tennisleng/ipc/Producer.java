package com.tennisleng.ipc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Producer process — writes messages into the shared ring buffer.
 *
 * Run this in one terminal:   ./gradlew runProducer
 * Run Consumer in another:    ./gradlew runConsumer
 *
 * They communicate through a memory-mapped file — no sockets, no serialization,
 * no kernel context switches. Just raw shared memory.
 */
public class Producer {

    // ── Configuration ───────────────────────────────────────────────────
    private static final Path SHARED_FILE = Path.of("/tmp/ipc-ring-buffer.dat");
    private static final int CAPACITY = 1024;          // slots (must be power of 2)
    private static final int MAX_PAYLOAD = 256;        // bytes per message

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║       PRODUCER — Low Latency IPC        ║");
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.println("║  Shared file: " + SHARED_FILE);
        System.out.println("║  Capacity:    " + CAPACITY + " slots");
        System.out.println("║  Max payload: " + MAX_PAYLOAD + " bytes");
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println();

        try (SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD)) {
            // Reset on startup (in prod, you'd recover from existing state)
            ring.reset();

            int messageCount = 0;
            while (true) {
                String message = "Message #" + messageCount + " @ " + System.nanoTime();
                byte[] payload = message.getBytes(StandardCharsets.UTF_8);

                // Spin-wait if buffer is full (busy-wait = lowest latency)
                // In a real system, you might back off or yield
                while (!ring.write(payload)) {
                    Thread.onSpinWait(); // CPU hint: "I'm spinning, save power"
                }

                if (messageCount % 10_000 == 0) {
                    System.out.println("[Producer] Sent: " + message);
                }

                messageCount++;

                // Throttle for demo purposes — remove for benchmarking
                if (args.length == 0 || !args[0].equals("--fast")) {
                    Thread.sleep(1);
                }
            }
        }
    }
}
