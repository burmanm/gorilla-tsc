package fi.iki.yak.ts.compression.gorilla;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 * @author Michael Burman
 */
public class EncodeTest {

    @Test
    void simpleEncodeAndDecodeTest() throws Exception {
        long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        ByteBufferBitOutput output = new ByteBufferBitOutput();

        Compressor c = new Compressor(now, output);

        Pair[] pairs = {
                new Pair(now + 10, Double.doubleToRawLongBits(1.0)),
                new Pair(now + 20, Double.doubleToRawLongBits(-2.0)),
                new Pair(now + 28, Double.doubleToRawLongBits(-2.5)),
                new Pair(now + 84, Double.doubleToRawLongBits(65537)),
                new Pair(now + 400, Double.doubleToRawLongBits(2147483650.0)),
                new Pair(now + 2300, Double.doubleToRawLongBits(-16384)),
                new Pair(now + 16384, Double.doubleToRawLongBits(2.8)),
                new Pair(now + 16500, Double.doubleToRawLongBits(-38.0))
        };

        Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getDoubleValue()));
        c.close();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        // Replace with stream once decompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Pair pair = d.readPair();
            assertEquals(pairs[i].getTimestamp(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(pairs[i].getDoubleValue(), pair.getDoubleValue(), "Value did not match");
        }

        assertNull(d.readPair());
    }

    /**
     * Tests encoding of similar floats, see https://github.com/dgryski/go-tsz/issues/4 for more information.
     */
    @Test
    void testEncodeSimilarFloats() throws Exception {
        long now = LocalDateTime.of(2015, Month.MARCH, 02, 00, 00).toInstant(ZoneOffset.UTC).toEpochMilli();

        ByteBufferBitOutput output = new ByteBufferBitOutput();
        Compressor c = new Compressor(now, output);

        ByteBuffer bb = ByteBuffer.allocate(5 * 2*Long.BYTES);

        bb.putLong(now + 1);
        bb.putDouble(6.00065e+06);
        bb.putLong(now + 2);
        bb.putDouble(6.000656e+06);
        bb.putLong(now + 3);
        bb.putDouble(6.000657e+06);
        bb.putLong(now + 4);
        bb.putDouble(6.000659e+06);
        bb.putLong(now + 5);
        bb.putDouble(6.000661e+06);

        bb.flip();

        for(int j = 0; j < 5; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        // Replace with stream once decompressor supports it
        for(int i = 0; i < 5; i++) {
            Pair pair = d.readPair();
            assertEquals(bb.getLong(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(bb.getDouble(), pair.getDoubleValue(), "Value did not match");
        }
        assertNull(d.readPair());
    }

    /**
     * Tests writing enough large amount of datapoints that causes the included ByteBufferBitOutput to do
     * internal byte array expansion.
     */
    @Test
    void testEncodeLargeAmountOfData() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 10000;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        ByteBufferBitOutput output = new ByteBufferBitOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putDouble(i * Math.random());
        }

        Compressor c = new Compressor(blockStart, output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        for(int i = 0; i < amountOfPoints; i++) {
            long tStamp = bb.getLong();
            double val = bb.getDouble();
            Pair pair = d.readPair();
            assertEquals(tStamp, pair.getTimestamp(), "Expected timestamp did not match at point " + i);
            assertEquals(val, pair.getDoubleValue());
        }
        assertNull(d.readPair());
    }

    @Test
    void testEmptyBlock() throws Exception {
        long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        ByteBufferBitOutput output = new ByteBufferBitOutput();

        Compressor c = new Compressor(now, output);
        c.close();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        assertNull(d.readPair());
    }

    @Test
    void testLongEncoding() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 10000;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        ByteBufferBitOutput output = new ByteBufferBitOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putLong(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE));
        }

        Compressor c = new Compressor(blockStart, output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getLong());
        }

        c.close();

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        for(int i = 0; i < amountOfPoints; i++) {
            long tStamp = bb.getLong();
            long val = bb.getLong();
            Pair pair = d.readPair();
            assertEquals(tStamp, pair.getTimestamp(), "Expected timestamp did not match at point " + i);
            assertEquals(val, pair.getLongValue());
        }
        assertNull(d.readPair());
    }
}
