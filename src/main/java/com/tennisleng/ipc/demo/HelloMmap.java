package com.tennisleng.ipc.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Week 2 "Hello World" — Process A writes a long, Process B reads it instantly.
 * No FileInputStream, no serialization. Just raw shared memory via mmap.
 *
 * This is the Java equivalent of C's mmap():
 *   C:    void* ptr = mmap(fd, ...);  *(long*)ptr = 42;
 *   Java: MappedByteBuffer buf = channel.map(...);  buf.putLong(0, 42);
 */
public class HelloMmap {

    private static final Path FILE = Path.of("/tmp/hello-mmap.dat");

    public static void main(String[] args) throws IOException {
        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 64);

            // WRITE: store a long at offset 0
            long value = System.nanoTime();
            buf.putLong(0, value);
            System.out.println("[Writer] Wrote: " + value);

            // READ: load it right back (in IPC, another process does this)
            long readBack = buf.getLong(0);
            System.out.println("[Reader] Read:  " + readBack);
            System.out.println("[Result] Match: " + (value == readBack));

            System.out.println("\nFile persisted at: " + FILE.toAbsolutePath());
            System.out.println("Another process can map the same file and read this value.");
        }
    }
}
