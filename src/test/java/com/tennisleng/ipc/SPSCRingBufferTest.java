package com.tennisleng.ipc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    // ═══════════════════════════════════════════════════════════════════
    //  BASIC READ / WRITE
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Basic read/write operations")
    class BasicOperations {

        @Test
        @DisplayName("write and read a single message")
        void writeAndReadSingleMessage() {
            byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
            assertTrue(ring.write(payload));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals("hello", view.getPayloadAsString());
        }

        @Test
        @DisplayName("read from empty buffer returns false")
        void readFromEmptyBufferReturnsFalse() {
            MessageView view = new MessageView();
            assertFalse(ring.read(view));
        }

        @Test
        @DisplayName("write returns false when buffer is full")
        void writeToFullBufferReturnsFalse() {
            byte[] payload = "x".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 16; i++) {
                assertTrue(ring.write(payload), "Write " + i + " should succeed");
            }
            assertFalse(ring.write(payload), "Write to full buffer should fail");
        }

        @Test
        @DisplayName("multiple messages preserve order (FIFO)")
        void messagesPreserveOrder() {
            ring.write("first".getBytes(StandardCharsets.UTF_8));
            ring.write("second".getBytes(StandardCharsets.UTF_8));
            ring.write("third".getBytes(StandardCharsets.UTF_8));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals("first", view.getPayloadAsString());

            assertTrue(ring.read(view));
            assertEquals("second", view.getPayloadAsString());

            assertTrue(ring.read(view));
            assertEquals("third", view.getPayloadAsString());

            assertFalse(ring.read(view)); // empty
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CAPACITY & WRAP-AROUND
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Capacity limits and wrap-around")
    class CapacityAndWrapAround {

        @Test
        @DisplayName("fill to capacity then drain completely")
        void fillAndDrainBuffer() {
            byte[] payload = "x".getBytes(StandardCharsets.UTF_8);

            // Fill
            for (int i = 0; i < 16; i++) {
                assertTrue(ring.write(payload));
            }
            assertFalse(ring.write(payload)); // full

            // Drain
            MessageView view = new MessageView();
            for (int i = 0; i < 16; i++) {
                assertTrue(ring.read(view));
            }
            assertFalse(ring.read(view)); // empty
        }

        @Test
        @DisplayName("wrap around correctly (write+read more than capacity)")
        void wrapAroundCorrectly() {
            MessageView view = new MessageView();

            // Process 64 messages through a 16-slot buffer = 4 full wraps
            for (int i = 0; i < 64; i++) {
                String msg = "msg-" + i;
                assertTrue(ring.write(msg.getBytes(StandardCharsets.UTF_8)),
                        "Write " + i + " should succeed");
                assertTrue(ring.read(view),
                        "Read " + i + " should succeed");
                assertEquals(msg, view.getPayloadAsString(),
                        "Message " + i + " content mismatch");
            }
        }

        @Test
        @DisplayName("wrap around with batched writes and reads")
        void batchedWrapAround() {
            MessageView view = new MessageView();

            // Do multiple rounds of batch-fill then batch-drain
            for (int round = 0; round < 5; round++) {
                // Fill 16 slots
                for (int i = 0; i < 16; i++) {
                    String msg = "r" + round + "-m" + i;
                    assertTrue(ring.write(msg.getBytes(StandardCharsets.UTF_8)));
                }
                assertFalse(ring.write("overflow".getBytes(StandardCharsets.UTF_8)));

                // Drain 16 slots and verify content
                for (int i = 0; i < 16; i++) {
                    assertTrue(ring.read(view));
                    assertEquals("r" + round + "-m" + i, view.getPayloadAsString());
                }
                assertFalse(ring.read(view));
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  PAYLOAD BOUNDARIES
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Payload size edge cases")
    class PayloadBoundaries {

        @Test
        @DisplayName("empty payload (0 bytes)")
        void emptyPayload() {
            byte[] empty = new byte[0];
            assertTrue(ring.write(empty));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals(0, view.getPayloadLength());
            assertEquals("", view.getPayloadAsString());
        }

        @Test
        @DisplayName("single byte payload")
        void singleBytePayload() {
            byte[] one = new byte[]{42};
            assertTrue(ring.write(one));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals(1, view.getPayloadLength());
            assertEquals(42, view.getByte(0));
        }

        @Test
        @DisplayName("maximum payload size (exactly max)")
        void maxPayloadSize() {
            byte[] maxPayload = new byte[64]; // ring was created with maxPayload=64
            for (int i = 0; i < 64; i++) maxPayload[i] = (byte) (i & 0xFF);

            assertTrue(ring.write(maxPayload));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals(64, view.getPayloadLength());

            // Verify every byte
            for (int i = 0; i < 64; i++) {
                assertEquals((byte) (i & 0xFF), view.getByte(i),
                        "Byte " + i + " mismatch");
            }
        }

        @Test
        @DisplayName("payload exceeding max throws exception")
        void payloadTooLargeThrows() {
            byte[] tooLarge = new byte[65]; // max is 64
            assertThrows(IllegalArgumentException.class, () -> ring.write(tooLarge));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  BYTEBUFFER WRITE PATH
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("ByteBuffer write overload")
    class ByteBufferWrite {

        @Test
        @DisplayName("write from ByteBuffer and read back")
        void writeFromByteBuffer() {
            ByteBuffer src = ByteBuffer.wrap("hello-bb".getBytes(StandardCharsets.UTF_8));
            assertTrue(ring.write(src));
            assertEquals(0, src.remaining(), "Source buffer should be fully consumed");

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals("hello-bb", view.getPayloadAsString());
        }

        @Test
        @DisplayName("write from ByteBuffer with offset")
        void writeFromByteBufferWithOffset() {
            byte[] data = "XXXhello".getBytes(StandardCharsets.UTF_8);
            ByteBuffer src = ByteBuffer.wrap(data, 3, 5); // just "hello"
            assertTrue(ring.write(src));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));
            assertEquals("hello", view.getPayloadAsString());
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  MESSAGE VIEW
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("MessageView typed accessors")
    class MessageViewTests {

        @Test
        @DisplayName("flyweight reuse — same view reads different messages")
        void flyweightReuse() {
            ring.write("aaa".getBytes(StandardCharsets.UTF_8));
            ring.write("bbb".getBytes(StandardCharsets.UTF_8));

            MessageView view = new MessageView();

            assertTrue(ring.read(view));
            assertEquals("aaa", view.getPayloadAsString());

            // Same view object, different message
            assertTrue(ring.read(view));
            assertEquals("bbb", view.getPayloadAsString());
        }

        @Test
        @DisplayName("copyPayloadTo with pre-allocated buffer")
        void copyPayloadToPreAllocated() {
            ring.write("test123".getBytes(StandardCharsets.UTF_8));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));

            byte[] dest = new byte[64]; // pre-allocated, larger than needed
            int copied = view.copyPayloadTo(dest);
            assertEquals(7, copied);
            assertEquals("test123", new String(dest, 0, copied, StandardCharsets.UTF_8));
        }

        @Test
        @DisplayName("copyPayloadTo with offset")
        void copyPayloadToWithOffset() {
            ring.write("data".getBytes(StandardCharsets.UTF_8));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));

            byte[] dest = new byte[64];
            dest[0] = 'H'; // pre-existing data
            int copied = view.copyPayloadTo(dest, 10);
            assertEquals(4, copied);
            assertEquals('H', dest[0]); // unchanged
            assertEquals('d', dest[10]);
        }

        @Test
        @DisplayName("isValid returns false before wrap")
        void isValidBeforeWrap() {
            MessageView view = new MessageView();
            assertFalse(view.isValid());
        }

        @Test
        @DisplayName("getPayloadCopy returns independent copy")
        void getPayloadCopyIsIndependent() {
            ring.write("original".getBytes(StandardCharsets.UTF_8));

            MessageView view = new MessageView();
            assertTrue(ring.read(view));

            byte[] copy = view.getPayloadCopy();
            assertEquals("original", new String(copy, StandardCharsets.UTF_8));

            // Mutating the copy shouldn't affect the buffer
            copy[0] = 'X';
            assertEquals('o', view.getByte(0)); // still 'o' in the buffer
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  INTROSPECTION / UTILITY
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Utility and introspection")
    class UtilityTests {

        @Test
        @DisplayName("size() reports correct count")
        void sizeReportsCorrectly() {
            assertEquals(0, ring.size());

            ring.write("a".getBytes(StandardCharsets.UTF_8));
            ring.write("b".getBytes(StandardCharsets.UTF_8));
            assertEquals(2, ring.size());

            MessageView view = new MessageView();
            ring.read(view);
            assertEquals(1, ring.size());
        }

        @Test
        @DisplayName("remainingCapacity() reports correctly")
        void remainingCapacityReportsCorrectly() {
            assertEquals(16, ring.remainingCapacity());

            ring.write("a".getBytes(StandardCharsets.UTF_8));
            assertEquals(15, ring.remainingCapacity());
        }

        @Test
        @DisplayName("reset clears buffer and sequences")
        void resetClearsEverything() {
            ring.write("data".getBytes(StandardCharsets.UTF_8));
            ring.write("more".getBytes(StandardCharsets.UTF_8));
            assertEquals(2, ring.size());

            ring.reset();
            assertEquals(0, ring.size());

            // Should be able to read nothing after reset
            MessageView view = new MessageView();
            assertFalse(ring.read(view));

            // Should be able to write fresh data
            assertTrue(ring.write("fresh".getBytes(StandardCharsets.UTF_8)));
            assertTrue(ring.read(view));
            assertEquals("fresh", view.getPayloadAsString());
        }

        @Test
        @DisplayName("getCapacity returns constructor value")
        void getCapacityReturnsCorrectValue() {
            assertEquals(16, ring.getCapacity());
        }

        @Test
        @DisplayName("getMaxPayloadSize returns constructor value")
        void getMaxPayloadSizeReturnsCorrectValue() {
            assertEquals(64, ring.getMaxPayloadSize());
        }

        @Test
        @DisplayName("toString includes useful info")
        void toStringIsInformative() {
            String s = ring.toString();
            assertTrue(s.contains("capacity=16"));
            assertTrue(s.contains("maxPayload=64"));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  VALIDATION
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Constructor validation")
    class ValidationTests {

        @Test
        @DisplayName("capacity must be power of 2")
        void capacityMustBePowerOfTwo() {
            assertThrows(IllegalArgumentException.class, () ->
                    new SPSCRingBuffer(tempFile, 10, 64));
        }

        @Test
        @DisplayName("capacity of 1 is valid (power of 2)")
        void capacityOfOneIsValid() throws IOException {
            try (SPSCRingBuffer tiny = new SPSCRingBuffer(tempFile, 1, 8)) {
                tiny.reset();
                assertTrue(tiny.write("hi".getBytes(StandardCharsets.UTF_8)));
                assertFalse(tiny.write("no".getBytes(StandardCharsets.UTF_8))); // full

                MessageView view = new MessageView();
                assertTrue(tiny.read(view));
                assertEquals("hi", view.getPayloadAsString());
            }
        }

        @Test
        @DisplayName("maxPayloadSize must be positive")
        void maxPayloadSizeMustBePositive() {
            assertThrows(IllegalArgumentException.class, () ->
                    new SPSCRingBuffer(tempFile, 16, 0));
            assertThrows(IllegalArgumentException.class, () ->
                    new SPSCRingBuffer(tempFile, 16, -1));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CONCURRENCY (single-threaded simulation of producer/consumer)
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Concurrent-style operations")
    class ConcurrencyTests {

        @Test
        @DisplayName("interleaved write/read simulating producer/consumer")
        void interleavedWriteRead() {
            MessageView view = new MessageView();

            // Simulate a realistic pattern: write a few, read a few, repeat
            for (int batch = 0; batch < 10; batch++) {
                // Write 3 messages
                for (int i = 0; i < 3; i++) {
                    String msg = "b" + batch + "-" + i;
                    assertTrue(ring.write(msg.getBytes(StandardCharsets.UTF_8)));
                }

                // Read 2 messages (leaving 1 in buffer)
                for (int i = 0; i < 2; i++) {
                    assertTrue(ring.read(view));
                    assertEquals("b" + batch + "-" + i, view.getPayloadAsString());
                }
            }

            // Drain remaining (10 messages left — one from each batch)
            int remaining = 0;
            while (ring.read(view)) {
                remaining++;
            }
            assertEquals(10, remaining);
        }

        @Test
        @DisplayName("stress test: 10000 messages through 16-slot buffer")
        void stressTest() {
            MessageView view = new MessageView();
            int total = 10_000;

            for (int i = 0; i < total; i++) {
                byte[] payload = ("stress-" + i).getBytes(StandardCharsets.UTF_8);
                assertTrue(ring.write(payload), "Write " + i + " failed");
                assertTrue(ring.read(view), "Read " + i + " failed");
                assertEquals("stress-" + i, view.getPayloadAsString(),
                        "Message " + i + " content mismatch");
            }
        }
    }
}
