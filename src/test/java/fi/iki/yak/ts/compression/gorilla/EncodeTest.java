package fi.iki.yak.ts.compression.gorilla;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

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
                new Pair(now + 10, 1.0),
                new Pair(now + 20, -2.0),
                new Pair(now + 28, -2.5),
                new Pair(now + 84, 65537),
                new Pair(now + 400, 2147483650.0),
                new Pair(now + 2300, -16384),
                new Pair(now + 16384, 2.8),
                new Pair(now + 16500, -38.0)
        };

        Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getValue()));
        c.close();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        // Replace with stream once decompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Pair pair = d.readPair();
            assertEquals(pairs[i].getTimestamp(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(pairs[i].getValue(), pair.getValue(), "Value did not match");
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

        Pair[] pairs = {
            new Pair(now + 1, 6.00065e+06),
                    new Pair(now + 2, 6.000656e+06),
                    new Pair(now + 3, 6.000657e+06),
                    new Pair(now + 4, 6.000659e+06),
                    new Pair(now + 5, 6.000661e+06)
        };

        Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getValue()));
        c.close();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor d = new Decompressor(input);

        // Replace with stream once decompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Pair pair = d.readPair();
            assertEquals(pairs[i].getTimestamp(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(pairs[i].getValue(), pair.getValue(), "Value did not match");
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
        int amountOfPoints = 1000;
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
            assertEquals(val, pair.getValue());
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
}
