package com.tennisleng.ipc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SPSCRingBufferTest {

    private Path tempFile;
    private SPSCRingBuffer ring;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = Files.createTempFile("spsc-test-", ".dat");
        ring = new SPSCRingBuffer(tempFile, 16, 64);
        ring.reset();
    }

    @AfterEach
    void tearDown() throws IOException {
        ring.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    void writeAndReadSingleMessage() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        assertTrue(ring.write(payload));

        MessageView view = new MessageView();
        assertTrue(ring.read(view));
        assertEquals("hello", view.getPayloadAsString());
    }

    @Test
    void readFromEmptyBufferReturnsFalse() {
        MessageView view = new MessageView();
        assertFalse(ring.read(view));
    }

    @Test
    void fillAndDrainBuffer() {
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);

        // Fill to capacity
        for (int i = 0; i < 16; i++) {
            assertTrue(ring.write(payload), "Write " + i + " should succeed");
        }
        // Next write should fail (buffer full)
        assertFalse(ring.write(payload));

        // Drain all messages
        MessageView view = new MessageView();
        for (int i = 0; i < 16; i++) {
            assertTrue(ring.read(view), "Read " + i + " should succeed");
        }
        // Next read should fail (buffer empty)
        assertFalse(ring.read(view));
    }

    @Test
    void wrapAroundCorrectly() {
        MessageView view = new MessageView();

        // Write and read 32 messages through a 16-slot buffer
        for (int i = 0; i < 32; i++) {
            String msg = "msg-" + i;
            assertTrue(ring.write(msg.getBytes(StandardCharsets.UTF_8)));
            assertTrue(ring.read(view));
            assertEquals(msg, view.getPayloadAsString());
        }
    }

    @Test
    void capacityMustBePowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () ->
                new SPSCRingBuffer(tempFile, 10, 64));
    }
}
