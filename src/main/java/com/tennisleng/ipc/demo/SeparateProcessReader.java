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
 * Polls a shared file for updates from SeparateProcessWriter.
 * Proves that two JVMs can talk through mmap with zero sockets.
 *
 *   ./gradlew runReader
 */
public class SeparateProcessReader {

    private static final Path FILE = Path.of("/tmp/ipc-simple-counter.dat");
    private static final VarHandle LONG_VH = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.nativeOrder());

    public static void main(String[] args) throws IOException {
        System.out.println("[Reader] file: " + FILE);
        System.out.println("[Reader] waiting for writer...");

        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 64);
            buf.order(ByteOrder.nativeOrder());

            long lastSeen = -1;
            long received = 0;

            while (true) {
                long current = (long) LONG_VH.getAcquire(buf, 0);
                if (current != lastSeen) {
                    received++;
                    if (received % 1000 == 0 || received <= 5) {
                        System.out.printf("[Reader] counter=%,d  (total %,d updates)%n",
                                current, received);
                    }
                    lastSeen = current;
                } else {
                    Thread.onSpinWait();
                }
            }
        }
    }
}
