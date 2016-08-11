package fi.iki.yak.ts.compression.gorilla.benchmark;

import fi.iki.yak.ts.compression.gorilla.Compressor;
import fi.iki.yak.ts.compression.gorilla.Decompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by michael on 8/11/16.
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10) // Reduce the amount of iterations if you start to see GC interference
public class EncodingBenchmark {

    @State(Scope.Benchmark)
    public static class DataGenerator {
        public List<Pair> insertList;

        @Param({"100000"})
        public int amountOfPoints;

        public long blockStart;

        public byte[] compressedBytes;
        public byte[] uncompressedBytes;

        @Setup(Level.Trial)
        public void setup() {
            blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            long now = blockStart + 60;

            insertList = new ArrayList<>(amountOfPoints);

            ByteBuffer bb = ByteBuffer.allocate(amountOfPoints * 2*Long.BYTES);

            for(int i = 0; i < amountOfPoints; i++) {
                now += 60;
                bb.putLong(now);
                bb.putLong(i);
            }

            if (bb.hasArray()) {
                final byte[] array = bb.array();
                final int arrayOffset = bb.arrayOffset();
                Arrays.copyOfRange(array, arrayOffset + bb.position(),
                        arrayOffset + bb.limit());
                uncompressedBytes = array;
            }


            Compressor c = new Compressor(blockStart);

            bb.flip();

            for(int j = 0; j < amountOfPoints; j++) {
                c.addValue(bb.getLong(), bb.getLong());
            }

            c.close();

            ByteBuffer buffer = c.getByteBuffer();
            if (buffer.hasArray()) {
                final byte[] array = buffer.array();
                final int arrayOffset = buffer.arrayOffset();
                Arrays.copyOfRange(array, arrayOffset + buffer.position(),
                        arrayOffset + buffer.limit());
                compressedBytes = array;
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmark(DataGenerator dg) {
        Compressor c = new Compressor(dg.blockStart);

        ByteBuffer bb = ByteBuffer.wrap(dg.uncompressedBytes);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getLong());
        }
        c.close();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingBenchmark(DataGenerator dg, Blackhole bh) throws Exception {
        Decompressor d = new Decompressor(dg.compressedBytes);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
    }
}
