package com.tennisleng.ipc;

import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SPSCRingBufferTest {

    private Path file;
    private SPSCRingBuffer ring;

    @BeforeEach
    void setUp() throws IOException {
        file = Files.createTempFile("spsc-test-", ".dat");
        ring = new SPSCRingBuffer(file, 16, 64);
        ring.reset();
    }

    @AfterEach
    void tearDown() throws IOException {
        ring.close();
        Files.deleteIfExists(file);
    }

    @Nested class BasicOps {

        @Test void singleWriteRead() {
            assertTrue(ring.write("hello".getBytes(StandardCharsets.UTF_8)));
            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            assertEquals("hello", v.getPayloadAsString());
        }

        @Test void emptyReadReturnsFalse() {
            assertFalse(ring.read(new MessageView()));
        }

        @Test void fullBufferReturnsFalse() {
            byte[] p = "x".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 16; i++) assertTrue(ring.write(p));
            assertFalse(ring.write(p));
        }

        @Test void fifoOrder() {
            ring.write("a".getBytes(StandardCharsets.UTF_8));
            ring.write("b".getBytes(StandardCharsets.UTF_8));
            ring.write("c".getBytes(StandardCharsets.UTF_8));

            MessageView v = new MessageView();
            ring.read(v); assertEquals("a", v.getPayloadAsString());
            ring.read(v); assertEquals("b", v.getPayloadAsString());
            ring.read(v); assertEquals("c", v.getPayloadAsString());
            assertFalse(ring.read(v));
        }
    }

    @Nested class WrapAround {

        @Test void fillDrain() {
            byte[] p = "x".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 16; i++) assertTrue(ring.write(p));
            assertFalse(ring.write(p));

            MessageView v = new MessageView();
            for (int i = 0; i < 16; i++) assertTrue(ring.read(v));
            assertFalse(ring.read(v));
        }

        @Test void wrapsFourTimes() {
            MessageView v = new MessageView();
            for (int i = 0; i < 64; i++) {
                String msg = "msg-" + i;
                assertTrue(ring.write(msg.getBytes(StandardCharsets.UTF_8)));
                assertTrue(ring.read(v));
                assertEquals(msg, v.getPayloadAsString());
            }
        }

        @Test void batchedRounds() {
            MessageView v = new MessageView();
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 16; i++) {
                    assertTrue(ring.write(("r" + round + "-" + i).getBytes(StandardCharsets.UTF_8)));
                }
                assertFalse(ring.write("overflow".getBytes(StandardCharsets.UTF_8)));

                for (int i = 0; i < 16; i++) {
                    assertTrue(ring.read(v));
                    assertEquals("r" + round + "-" + i, v.getPayloadAsString());
                }
                assertFalse(ring.read(v));
            }
        }
    }

    @Nested class PayloadEdgeCases {

        @Test void emptyPayload() {
            assertTrue(ring.write(new byte[0]));
            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            assertEquals(0, v.getPayloadLength());
        }

        @Test void singleByte() {
            assertTrue(ring.write(new byte[]{42}));
            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            assertEquals(42, v.getByte(0));
        }

        @Test void exactlyMaxSize() {
            byte[] max = new byte[64];
            for (int i = 0; i < 64; i++) max[i] = (byte) i;
            assertTrue(ring.write(max));

            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            for (int i = 0; i < 64; i++) assertEquals((byte) i, v.getByte(i));
        }

        @Test void tooLargeThrows() {
            assertThrows(IllegalArgumentException.class, () -> ring.write(new byte[65]));
        }
    }

    @Nested class ByteBufferWrite {

        @Test void basicByteBuffer() {
            ByteBuffer src = ByteBuffer.wrap("hello-bb".getBytes(StandardCharsets.UTF_8));
            assertTrue(ring.write(src));
            assertEquals(0, src.remaining());

            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            assertEquals("hello-bb", v.getPayloadAsString());
        }

        @Test void byteBufferWithOffset() {
            ByteBuffer src = ByteBuffer.wrap("XXXhello".getBytes(StandardCharsets.UTF_8), 3, 5);
            assertTrue(ring.write(src));
            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            assertEquals("hello", v.getPayloadAsString());
        }
    }

    @Nested class ViewTests {

        @Test void reusesSameObject() {
            ring.write("aaa".getBytes(StandardCharsets.UTF_8));
            ring.write("bbb".getBytes(StandardCharsets.UTF_8));

            MessageView v = new MessageView();
            ring.read(v); assertEquals("aaa", v.getPayloadAsString());
            ring.read(v); assertEquals("bbb", v.getPayloadAsString());
        }

        @Test void copyPayloadTo() {
            ring.write("test123".getBytes(StandardCharsets.UTF_8));
            MessageView v = new MessageView();
            ring.read(v);

            byte[] dest = new byte[64];
            assertEquals(7, v.copyPayloadTo(dest));
            assertEquals("test123", new String(dest, 0, 7, StandardCharsets.UTF_8));
        }

        @Test void copyPayloadToWithOffset() {
            ring.write("data".getBytes(StandardCharsets.UTF_8));
            MessageView v = new MessageView();
            ring.read(v);

            byte[] dest = new byte[64];
            dest[0] = 'Z';
            assertEquals(4, v.copyPayloadTo(dest, 10));
            assertEquals('Z', dest[0]); // untouched
            assertEquals('d', dest[10]);
        }

        @Test void isValidFalseBeforeWrap() {
            assertFalse(new MessageView().isValid());
        }

        @Test void payloadCopyIsIndependent() {
            ring.write("orig".getBytes(StandardCharsets.UTF_8));
            MessageView v = new MessageView();
            ring.read(v);

            byte[] copy = v.getPayloadCopy();
            copy[0] = 'X';
            assertEquals('o', v.getByte(0)); // buffer untouched
        }
    }

    @Nested class Utility {

        @Test void size() {
            assertEquals(0, ring.size());
            ring.write("a".getBytes(StandardCharsets.UTF_8));
            ring.write("b".getBytes(StandardCharsets.UTF_8));
            assertEquals(2, ring.size());
            ring.read(new MessageView());
            assertEquals(1, ring.size());
        }

        @Test void remainingCapacity() {
            assertEquals(16, ring.remainingCapacity());
            ring.write("a".getBytes(StandardCharsets.UTF_8));
            assertEquals(15, ring.remainingCapacity());
        }

        @Test void resetWorks() {
            ring.write("data".getBytes(StandardCharsets.UTF_8));
            ring.reset();
            assertEquals(0, ring.size());
            assertFalse(ring.read(new MessageView()));

            assertTrue(ring.write("fresh".getBytes(StandardCharsets.UTF_8)));
            MessageView v = new MessageView();
            assertTrue(ring.read(v));
            assertEquals("fresh", v.getPayloadAsString());
        }

        @Test void getters() {
            assertEquals(16, ring.getCapacity());
            assertEquals(64, ring.getMaxPayloadSize());
        }
    }

    @Nested class Validation {

        @Test void notPowerOfTwo() {
            assertThrows(IllegalArgumentException.class, () ->
                    new SPSCRingBuffer(file, 10, 64));
        }

        @Test void capacityOne() throws IOException {
            try (var tiny = new SPSCRingBuffer(file, 1, 8)) {
                tiny.reset();
                assertTrue(tiny.write("hi".getBytes(StandardCharsets.UTF_8)));
                assertFalse(tiny.write("no".getBytes(StandardCharsets.UTF_8)));
                MessageView v = new MessageView();
                assertTrue(tiny.read(v));
                assertEquals("hi", v.getPayloadAsString());
            }
        }

        @Test void maxPayloadMustBePositive() {
            assertThrows(IllegalArgumentException.class, () -> new SPSCRingBuffer(file, 16, 0));
            assertThrows(IllegalArgumentException.class, () -> new SPSCRingBuffer(file, 16, -1));
        }
    }

    @Nested class Stress {

        @Test void interleavedPattern() {
            MessageView v = new MessageView();
            for (int batch = 0; batch < 10; batch++) {
                for (int i = 0; i < 3; i++)
                    assertTrue(ring.write(("b" + batch + "-" + i).getBytes(StandardCharsets.UTF_8)));
                for (int i = 0; i < 2; i++) {
                    assertTrue(ring.read(v));
                    assertEquals("b" + batch + "-" + i, v.getPayloadAsString());
                }
            }
            // drain the remaining 10
            int left = 0;
            while (ring.read(v)) left++;
            assertEquals(10, left);
        }

        @Test void tenThousandMessages() {
            MessageView v = new MessageView();
            for (int i = 0; i < 10_000; i++) {
                assertTrue(ring.write(("s-" + i).getBytes(StandardCharsets.UTF_8)));
                assertTrue(ring.read(v));
                assertEquals("s-" + i, v.getPayloadAsString());
            }
        }
    }
}
