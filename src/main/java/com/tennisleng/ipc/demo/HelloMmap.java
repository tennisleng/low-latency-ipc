package com.tennisleng.ipc.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Week 2 "Hello World" — the simplest possible mmap demo.
 *
 * This demonstrates the core concept: two "processes" (simulated here in
 * one JVM) sharing data through a memory-mapped file with NO serialization,
 * NO sockets, and NO copying.
 *
 * The Java equivalent of C's mmap():
 *   C:    void* ptr = mmap(fd, ...);  *(long*)ptr = 42;
 *   Java: MappedByteBuffer buf = channel.map(...);  buf.putLong(0, 42);
 *
 * Run: ./gradlew runHelloMmap
 */
public class HelloMmap {

    private static final Path FILE = Path.of("/tmp/hello-mmap.dat");

    // VarHandle for demonstrating memory barriers even in this simple example
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.nativeOrder());

    public static void main(String[] args) throws IOException {
        System.out.println("┌──────────────────────────────────────────┐");
        System.out.println("│    Memory-Mapped File — Hello World     │");
        System.out.println("└──────────────────────────────────────────┘");
        System.out.println();

        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            // Map 128 bytes of the file into memory.
            // After this call, 'buf' IS the file. Writes to buf go straight
            // to the file (and vice versa). No flush needed.
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 128);
            buf.order(ByteOrder.nativeOrder());

            System.out.println("=== Demo 1: Basic read/write ===");
            System.out.println();

            // WRITE: store a long at offset 0 (plain store — no barrier)
            long value = System.nanoTime();
            buf.putLong(0, value);
            System.out.println("  [Writer] Stored value at offset 0:  " + value);

            // READ: load it back (plain load)
            long readBack = buf.getLong(0);
            System.out.println("  [Reader] Loaded value from offset 0: " + readBack);
            System.out.println("  [Check]  Match: " + (value == readBack));
            System.out.println();

            // ── Demo 2: Multiple fields (simulating a structured message) ──
            System.out.println("=== Demo 2: Structured message ===");
            System.out.println();

            // Layout:
            //   offset  0: timestamp (long, 8 bytes)
            //   offset  8: price     (double, 8 bytes)
            //   offset 16: quantity  (int, 4 bytes)
            //   offset 20: symbol    (4 bytes, ASCII)

            long timestamp = System.nanoTime();
            double price = 152.37;
            int quantity = 100;
            byte[] symbol = "AAPL".getBytes();

            // Write each field at its offset
            buf.putLong(0, timestamp);
            buf.putDouble(8, price);
            buf.putInt(16, quantity);
            for (int i = 0; i < symbol.length; i++) {
                buf.put(20 + i, symbol[i]);
            }

            System.out.println("  [Writer] Wrote order:");
            System.out.printf("    timestamp = %d%n", timestamp);
            System.out.printf("    price     = %.2f%n", price);
            System.out.printf("    quantity  = %d%n", quantity);
            System.out.printf("    symbol    = AAPL%n");
            System.out.println();

            // Read it all back (as a "consumer" would)
            long ts2 = buf.getLong(0);
            double pr2 = buf.getDouble(8);
            int qty2 = buf.getInt(16);
            byte[] sym2 = new byte[4];
            for (int i = 0; i < 4; i++) sym2[i] = buf.get(20 + i);

            System.out.println("  [Reader] Read order:");
            System.out.printf("    timestamp = %d%n", ts2);
            System.out.printf("    price     = %.2f%n", pr2);
            System.out.printf("    quantity  = %d%n", qty2);
            System.out.printf("    symbol    = %s%n", new String(sym2));
            System.out.println();

            // ── Demo 3: VarHandle with memory barrier ──────────────────────
            System.out.println("=== Demo 3: VarHandle acquire/release ===");
            System.out.println();

            // RELEASE store: guarantees all prior writes are visible
            // before this value becomes visible to an ACQUIRE load.
            long seq = 42L;
            LONG_VIEW.setRelease(buf, 64, seq);
            System.out.println("  [Writer] setRelease(offset=64, value=" + seq + ")");

            // ACQUIRE load: guarantees we see all writes that happened
            // before the corresponding RELEASE store.
            long readSeq = (long) LONG_VIEW.getAcquire(buf, 64);
            System.out.println("  [Reader] getAcquire(offset=64) = " + readSeq);
            System.out.println("  [Check]  Match: " + (seq == readSeq));
            System.out.println();

            System.out.println("File persisted at: " + FILE.toAbsolutePath());
            System.out.println("Another JVM can map the same file and read these values.");
            System.out.println();
            System.out.println("Next step: look at SPSCRingBuffer.java to see how this");
            System.out.println("basic mechanism scales into a full IPC queue.");
        }
    }
}
