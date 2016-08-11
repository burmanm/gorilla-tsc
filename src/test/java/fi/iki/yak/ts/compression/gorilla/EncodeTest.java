package fi.iki.yak.ts.compression.gorilla;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Michael Burman
 */
public class EncodeTest {

    @Test
    void simpleEncodeAndDecodeTest() throws Exception {
        long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        Compressor c = new Compressor(now);

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

        Decompressor d = new Decompressor(c.getByteBuffer().array());

        // Replace with stream once decompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Pair pair = d.readPair();
            assertEquals(pairs[i].getTimestamp(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(pairs[i].getValue(), pair.getValue(), "Value did not match");
        }

        assertNull(d.readPair());
    }

    @Test
    void testEncodeSimilarFloats() throws Exception {
        // See https://github.com/dgryski/go-tsz/issues/4
        long now = LocalDateTime.of(2015, Month.MARCH, 02, 00, 00).toInstant(ZoneOffset.UTC).toEpochMilli();

        Compressor c = new Compressor(now);

        Pair[] pairs = {
            new Pair(now + 1, 6.00065e+06),
                    new Pair(now + 2, 6.000656e+06),
                    new Pair(now + 3, 6.000657e+06),
                    new Pair(now + 4, 6.000659e+06),
                    new Pair(now + 5, 6.000661e+06)
        };

        Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getValue()));
        c.close();

        Decompressor d = new Decompressor(c.getByteBuffer().array());

        // Replace with stream once decompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Pair pair = d.readPair();
            assertEquals(pairs[i].getTimestamp(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(pairs[i].getValue(), pair.getValue(), "Value did not match");
        }
        assertNull(d.readPair());
    }
}
