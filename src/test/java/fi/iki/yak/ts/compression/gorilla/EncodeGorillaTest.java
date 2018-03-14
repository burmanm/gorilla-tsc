package fi.iki.yak.ts.compression.gorilla;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

import fi.iki.yak.ts.compression.gorilla.predictors.DifferentialFCM;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 * @author Michael Burman
 */
public class EncodeGorillaTest {

    private void comparePairsToCompression(long blockTimestamp, Pair[] pairs) {
        LongArrayOutput output = new LongArrayOutput();

        GorillaCompressor c = new GorillaCompressor(blockTimestamp, output);

        Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getDoubleValue()));
        c.close();

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input);

        // Replace with stream once GorillaDecompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Pair pair = d.readPair();
            assertEquals(pairs[i].getTimestamp(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(pairs[i].getDoubleValue(), pair.getDoubleValue(), "Value did not match");
        }

        assertNull(d.readPair());
    }

    @Test
    void simpleEncodeAndDecodeTest() throws Exception {
        long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

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

        comparePairsToCompression(now, pairs);
    }

    @Test
    public void willItBlend() throws Exception {
        long blockTimestamp = 1500400800000L;

        Pair[] pairs = {
                new Pair(1500405481623L, 69087),
                new Pair(1500405488693L, 65640),
                new Pair(1500405495993L, 58155),
                new Pair(1500405503743L, 61025),
                new Pair(1500405511623L, 91156),
                new Pair(1500405519803L, 37516),
                new Pair(1500405528313L, 93515),
                new Pair(1500405537233L, 96226),
                new Pair(1500405546453L, 23833),
                new Pair(1500405556103L, 73186),
                new Pair(1500405566143L, 96947),
                new Pair(1500405576163L, 46927),
                new Pair(1500405586173L, 77954),
                new Pair(1500405596183L, 29302),
                new Pair(1500405606213L, 6700),
                new Pair(1500405616163L, 71971),
                new Pair(1500405625813L, 8528),
                new Pair(1500405635763L, 85321),
                new Pair(1500405645634L, 83229),
                new Pair(1500405655633L, 78298),
                new Pair(1500405665623L, 87122),
                new Pair(1500405675623L, 82055),
                new Pair(1500405685723L, 75067),
                new Pair(1500405695663L, 33680),
                new Pair(1500405705743L, 17576),
                new Pair(1500405715813L, 89701),
                new Pair(1500405725773L, 21427),
                new Pair(1500405735883L, 58255),
                new Pair(1500405745903L, 3768),
                new Pair(1500405755863L, 62086),
                new Pair(1500405765843L, 66965),
                new Pair(1500405775773L, 35801),
                new Pair(1500405785883L, 72169),
                new Pair(1500405795843L, 43089),
                new Pair(1500405805733L, 31418),
                new Pair(1500405815853L, 84781),
                new Pair(1500405825963L, 36103),
                new Pair(1500405836004L, 87431),
                new Pair(1500405845953L, 7379),
                new Pair(1500405855913L, 66919),
                new Pair(1500405865963L, 30906),
                new Pair(1500405875953L, 88630),
                new Pair(1500405885943L, 27546),
                new Pair(1500405896033L, 43813),
                new Pair(1500405906094L, 2124),
                new Pair(1500405916063L, 49399),
                new Pair(1500405926143L, 94577),
                new Pair(1500405936123L, 98459),
                new Pair(1500405946033L, 49457),
                new Pair(1500405956023L, 92838),
                new Pair(1500405966023L, 15628),
                new Pair(1500405976043L, 53916),
                new Pair(1500405986063L, 90387),
                new Pair(1500405996123L, 43176),
                new Pair(1500406006123L, 18838),
                new Pair(1500406016174L, 78847),
                new Pair(1500406026173L, 39591),
                new Pair(1500406036004L, 77070),
                new Pair(1500406045964L, 56788),
                new Pair(1500406056043L, 96706),
                new Pair(1500406066123L, 20756),
                new Pair(1500406076113L, 64433),
                new Pair(1500406086133L, 45791),
                new Pair(1500406096123L, 75028),
                new Pair(1500406106193L, 55403),
                new Pair(1500406116213L, 36991),
                new Pair(1500406126073L, 92929),
                new Pair(1500406136103L, 60416),
                new Pair(1500406146183L, 55485),
                new Pair(1500406156383L, 53525),
                new Pair(1500406166313L, 96021),
                new Pair(1500406176414L, 22705),
                new Pair(1500406186613L, 89801),
                new Pair(1500406196543L, 51975),
                new Pair(1500406206483L, 86741),
                new Pair(1500406216483L, 22440),
                new Pair(1500406226433L, 51818),
                new Pair(1500406236403L, 61965),
                new Pair(1500406246413L, 19074),
                new Pair(1500406256494L, 54521),
                new Pair(1500406266413L, 59315),
                new Pair(1500406276303L, 19171),
                new Pair(1500406286213L, 98800),
                new Pair(1500406296183L, 7086),
                new Pair(1500406306103L, 60578),
                new Pair(1500406316073L, 96828),
                new Pair(1500406326143L, 83746),
                new Pair(1500406336153L, 85481),
                new Pair(1500406346113L, 22346),
                new Pair(1500406356133L, 80976),
                new Pair(1500406366065L, 43586),
                new Pair(1500406376074L, 82500),
                new Pair(1500406386184L, 13576),
                new Pair(1500406396113L, 77871),
                new Pair(1500406406094L, 60978),
                new Pair(1500406416203L, 35264),
                new Pair(1500406426323L, 79733),
                new Pair(1500406436343L, 29140),
                new Pair(1500406446323L, 7237),
                new Pair(1500406456344L, 52866),
                new Pair(1500406466393L, 88456),
                new Pair(1500406476493L, 33533),
                new Pair(1500406486524L, 96961),
                new Pair(1500406496453L, 16389),
                new Pair(1500406506453L, 31181),
                new Pair(1500406516433L, 63282),
                new Pair(1500406526433L, 92857),
                new Pair(1500406536413L, 4582),
                new Pair(1500406546383L, 46832),
                new Pair(1500406556473L, 6335),
                new Pair(1500406566413L, 44367),
                new Pair(1500406576513L, 84640),
                new Pair(1500406586523L, 36174),
                new Pair(1500406596553L, 40075),
                new Pair(1500406606603L, 80886),
                new Pair(1500406616623L, 43784),
                new Pair(1500406626623L, 25077),
                new Pair(1500406636723L, 18617),
                new Pair(1500406646723L, 72681),
                new Pair(1500406656723L, 84811),
                new Pair(1500406666783L, 90053),
                new Pair(1500406676685L, 25708),
                new Pair(1500406686713L, 57134),
                new Pair(1500406696673L, 87193),
                new Pair(1500406706743L, 66057),
                new Pair(1500406716724L, 51404),
                new Pair(1500406726753L, 90141),
                new Pair(1500406736813L, 10434),
                new Pair(1500406746803L, 29056),
                new Pair(1500406756833L, 48160),
                new Pair(1500406766924L, 96652),
                new Pair(1500406777113L, 64141),
                new Pair(1500406787113L, 22143),
                new Pair(1500406797093L, 20561),
                new Pair(1500406807113L, 66401),
                new Pair(1500406817283L, 76802),
                new Pair(1500406827284L, 37555),
                new Pair(1500406837323L, 63169),
                new Pair(1500406847463L, 45712),
                new Pair(1500406857513L, 44751),
                new Pair(1500406867523L, 98891),
                new Pair(1500406877523L, 38122),
                new Pair(1500406887623L, 46202),
                new Pair(1500406897703L, 5875),
                new Pair(1500406907663L, 17397),
                new Pair(1500406917603L, 39994),
                new Pair(1500406927633L, 82385),
                new Pair(1500406937623L, 15598),
                new Pair(1500406947693L, 36235),
                new Pair(1500406957703L, 97536),
                new Pair(1500406967673L, 28557),
                new Pair(1500406977723L, 13985),
                new Pair(1500406987663L, 64304),
                new Pair(1500406997573L, 83693),
                new Pair(1500407007494L, 6574),
                new Pair(1500407017493L, 25134),
                new Pair(1500407027503L, 50383),
                new Pair(1500407037523L, 55922),
                new Pair(1500407047603L, 73436),
                new Pair(1500407057473L, 68235),
                new Pair(1500407067553L, 1469),
                new Pair(1500407077463L, 44315),
                new Pair(1500407087463L, 95064),
                new Pair(1500407097443L, 1997),
                new Pair(1500407107473L, 17247),
                new Pair(1500407117453L, 42454),
                new Pair(1500407127413L, 73631),
                new Pair(1500407137363L, 96890),
                new Pair(1500407147343L, 43450),
                new Pair(1500407157363L, 42042),
                new Pair(1500407167403L, 83014),
                new Pair(1500407177473L, 32051),
                new Pair(1500407187523L, 69280),
                new Pair(1500407197495L, 21425),
                new Pair(1500407207453L, 93748),
                new Pair(1500407217413L, 64151),
                new Pair(1500407227443L, 38791),
                new Pair(1500407237463L, 5248),
                new Pair(1500407247523L, 92935),
                new Pair(1500407257513L, 18516),
                new Pair(1500407267584L, 98870),
                new Pair(1500407277573L, 82244),
                new Pair(1500407287723L, 65464),
                new Pair(1500407297723L, 33801),
                new Pair(1500407307673L, 18331),
                new Pair(1500407317613L, 89744),
                new Pair(1500407327553L, 98460),
                new Pair(1500407337503L, 24709),
                new Pair(1500407347423L, 8407),
                new Pair(1500407357383L, 69451),
                new Pair(1500407367333L, 51100),
                new Pair(1500407377373L, 25309),
                new Pair(1500407387443L, 16148),
                new Pair(1500407397453L, 98974),
                new Pair(1500407407543L, 80284),
                new Pair(1500407417583L, 170),
                new Pair(1500407427453L, 34706),
                new Pair(1500407437433L, 39681),
                new Pair(1500407447603L, 6140),
                new Pair(1500407457513L, 64595),
                new Pair(1500407467564L, 59862),
                new Pair(1500407477563L, 53795),
                new Pair(1500407487593L, 83493),
                new Pair(1500407497584L, 90639),
                new Pair(1500407507623L, 16777),
                new Pair(1500407517613L, 11096),
                new Pair(1500407527673L, 38512),
                new Pair(1500407537963L, 52759),
                new Pair(1500407548023L, 79567),
                new Pair(1500407558033L, 48664),
                new Pair(1500407568113L, 10710),
                new Pair(1500407578164L, 25635),
                new Pair(1500407588213L, 40985),
                new Pair(1500407598163L, 94089),
                new Pair(1500407608163L, 50056),
                new Pair(1500407618223L, 15550),
                new Pair(1500407628143L, 78823),
                new Pair(1500407638223L, 9044),
                new Pair(1500407648173L, 20782),
                new Pair(1500407658023L, 86390),
                new Pair(1500407667903L, 79444),
                new Pair(1500407677903L, 84051),
                new Pair(1500407687923L, 91554),
                new Pair(1500407697913L, 58777),
                new Pair(1500407708003L, 89474),
                new Pair(1500407718083L, 94026),
                new Pair(1500407728034L, 41613),
                new Pair(1500407738083L, 64667),
                new Pair(1500407748034L, 5160),
                new Pair(1500407758003L, 45140),
                new Pair(1500407768033L, 53704),
                new Pair(1500407778083L, 68097),
                new Pair(1500407788043L, 81137),
                new Pair(1500407798023L, 59657),
                new Pair(1500407808033L, 56572),
                new Pair(1500407817983L, 1993),
                new Pair(1500407828063L, 62608),
                new Pair(1500407838213L, 76489),
                new Pair(1500407848203L, 22147),
                new Pair(1500407858253L, 92829),
                new Pair(1500407868073L, 48499),
                new Pair(1500407878053L, 89152),
                new Pair(1500407888073L, 9191),
                new Pair(1500407898033L, 49881),
                new Pair(1500407908113L, 96020),
                new Pair(1500407918213L, 90203),
                new Pair(1500407928234L, 32217),
                new Pair(1500407938253L, 94302),
                new Pair(1500407948293L, 83111),
                new Pair(1500407958234L, 75576),
                new Pair(1500407968073L, 5973),
                new Pair(1500407978023L, 5175),
                new Pair(1500407987923L, 63350),
                new Pair(1500407997833L, 44081)
        };

        comparePairsToCompression(blockTimestamp, pairs);
    }

    /**
     * Tests encoding of similar floats, see https://github.com/dgryski/go-tsz/issues/4 for more information.
     */
    @Test
    void testEncodeSimilarFloats() throws Exception {
        long now = LocalDateTime.of(2015, Month.MARCH, 02, 00, 00).toInstant(ZoneOffset.UTC).toEpochMilli();

        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(now, output);

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

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input);

        // Replace with stream once GorillaDecompressor supports it
        for(int i = 0; i < 5; i++) {
            Pair pair = d.readPair();
            assertEquals(bb.getLong(), pair.getTimestamp(), "Timestamp did not match");
            assertEquals(bb.getDouble(), pair.getDoubleValue(), "Value did not match");
        }
        assertNull(d.readPair());
    }

    /**
     * Tests writing enough large amount of datapoints that causes the included LongArrayOutput to do
     * internal byte array expansion.
     */
    @Test
    void testEncodeLargeAmountOfData() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 100000;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putDouble(i * Math.random());
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input);

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
    void testEncodeLargeAmountOfDataOldBuffer() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 100000;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        ByteBufferBitOutput output = new ByteBufferBitOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putDouble(i * Math.random());
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        GorillaDecompressor d = new GorillaDecompressor(input);

        for(int i = 0; i < amountOfPoints; i++) {
            long tStamp = bb.getLong();
            double val = bb.getDouble();
            Pair pair = d.readPair();
            assertEquals(tStamp, pair.getTimestamp(), "Expected timestamp did not match at point " + i);
            assertEquals(val, pair.getDoubleValue());
        }
        assertNull(d.readPair());
    }

    /**
     * Although not intended usage, an empty block should not cause errors
     */
    @Test
    void testEmptyBlock() throws Exception {
        long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        LongArrayOutput output = new LongArrayOutput();

        GorillaCompressor c = new GorillaCompressor(now, output);
        c.close();

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input);

        assertNull(d.readPair());
    }

    @Test
    void testCopyFlush() {
        long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        LongArrayOutput output = new LongArrayOutput();

        GorillaCompressor c = new GorillaCompressor(now, output);

        c.addValue(now + 1, 1.0);
        c.addValue(now + 2, 1.0);

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input);

        assertEquals(now + 1, d.readPair().getTimestamp());
        assertEquals(now + 2, d.readPair().getTimestamp());
    }

    /**
     * Long values should be compressable and decompressable in the stream
     */
    @Test
    void testLongEncoding() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 10000;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putLong(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getLong());
        }

        c.close();

        bb.flip();

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input);

        for(int i = 0; i < amountOfPoints; i++) {
            long tStamp = bb.getLong();
            long val = bb.getLong();
            Pair pair = d.readPair();
            assertEquals(tStamp, pair.getTimestamp(), "Expected timestamp did not match at point " + i);
            assertEquals(val, pair.getLongValue());
        }
        assertNull(d.readPair());
    }

    /**
     * Tests writing enough large amount of datapoints that causes the included LongArrayOutput to do
     * internal byte array expansion.
     */
    @Test
    void testDifferentialFCM() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 100000;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putDouble(i * Math.random());
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output, new DifferentialFCM(1024));

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        LongArrayInput input = new LongArrayInput(output.getLongArray());
        GorillaDecompressor d = new GorillaDecompressor(input, new DifferentialFCM(1024));

        for(int i = 0; i < amountOfPoints; i++) {
            long tStamp = bb.getLong();
            double val = bb.getDouble();
            Pair pair = d.readPair();
            assertEquals(tStamp, pair.getTimestamp(), "Expected timestamp did not match at point " + i);
            assertEquals(val, pair.getDoubleValue());
        }
        assertNull(d.readPair());
    }

}
