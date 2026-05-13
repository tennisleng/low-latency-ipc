package com.tennisleng.ipc.demo;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Bare-minimum mmap example. Basically: "can I write a long to a file
 * and read it back without FileInputStream?"
 *
 * Yes. That's what FileChannel.map() does. It's mmap() with extra steps.
 *
 *   ./gradlew runHelloMmap
 */
public class HelloMmap {

    private static final Path FILE = Path.of("/tmp/hello-mmap.dat");

    private static final VarHandle LONG_VH = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.nativeOrder());

    public static void main(String[] args) throws IOException {
        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 128);
            buf.order(ByteOrder.nativeOrder());

            // 1 — basic read/write
            System.out.println("--- basic put/get ---");
            long val = System.nanoTime();
            buf.putLong(0, val);
            System.out.println("  wrote: " + val);
            System.out.println("  read:  " + buf.getLong(0));

            // 2 — structured message (like a simple order)
            System.out.println("\n--- structured message ---");
            buf.putLong(0, System.nanoTime());      // timestamp
            buf.putDouble(8, 152.37);                // price
            buf.putInt(16, 100);                     // qty
            byte[] sym = "AAPL".getBytes();
            for (int i = 0; i < sym.length; i++) buf.put(20 + i, sym[i]);

            System.out.printf("  ts=%d  price=%.2f  qty=%d  sym=%s%n",
                    buf.getLong(0), buf.getDouble(8), buf.getInt(16),
                    new String(new byte[]{buf.get(20), buf.get(21), buf.get(22), buf.get(23)}));

            // 3 — VarHandle acquire/release
            System.out.println("\n--- VarHandle barriers ---");
            LONG_VH.setRelease(buf, 64, 42L);
            long got = (long) LONG_VH.getAcquire(buf, 64);
            System.out.println("  setRelease(64, 42), getAcquire(64) = " + got);

            System.out.println("\nfile: " + FILE.toAbsolutePath());
            System.out.println("another JVM can map this same file to read these values.");
        }
    }
}
