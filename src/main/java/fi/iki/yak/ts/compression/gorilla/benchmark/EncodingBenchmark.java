package fi.iki.yak.ts.compression.gorilla.benchmark;

import fi.iki.yak.ts.compression.gorilla.*;
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

        public ByteBuffer uncompressedBuffer;
        public ByteBuffer compressedBuffer;

        public ByteBufferBitOutput output;
        public ByteBufferBitInput input;

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
                uncompressedBuffer = ByteBuffer.wrap(array);
            }
            output = new ByteBufferBitOutput();

            Compressor c = new Compressor(blockStart, output);

            bb.flip();

            for(int j = 0; j < amountOfPoints; j++) {
                c.addValue(bb.getLong(), bb.getLong());
            }

            c.close();

            ByteBuffer byteBuffer = output.getByteBuffer();
            byteBuffer.flip();
            compressedBuffer = byteBuffer;

            input = new ByteBufferBitInput(byteBuffer.duplicate());

        }
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmark(DataGenerator dg) {
        dg.output.getByteBuffer().clear();
        Compressor c = new Compressor(dg.blockStart, dg.output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedBuffer.getLong(), dg.uncompressedBuffer.getLong());
        }
        dg.uncompressedBuffer.flip();
        c.close();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingBenchmark(DataGenerator dg, Blackhole bh) throws Exception {
        Decompressor d = new Decompressor(dg.input);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
        dg.input.getByteBuffer().flip();
    }
}
