package fi.iki.yak.ts.compression.gorilla.predictors;

import fi.iki.yak.ts.compression.gorilla.Predictor;

/**
 * Differential Finite Context Method (DFCM) is a context based predictor.
 *
 * @author Michael Burman
 */
public class DifferentialFCM implements Predictor {

    private long lastValue = 0L;
    private final long[] table;
    private int lastHash = 0;

    private final int mask;

    /**
     * Create a new DFCM predictor
     *
     * @param size Prediction table size, will be rounded to the next power of two and must be larger than 0
     */
    public DifferentialFCM(int size) {
        if(size > 0) {
            size--;
            int leadingZeros = Long.numberOfLeadingZeros(size);
            int newSize = 1 << (Long.SIZE - leadingZeros);

            this.table = new long[newSize];
            this.mask = newSize - 1;
        } else {
            throw new IllegalArgumentException("Size must be positive");
        }
    }

    @Override
    public void update(long value) {
        table[lastHash] = value - lastValue;
        lastHash = (int) (((lastHash << 5) ^ ((value - lastValue) >> 50)) & this.mask);
        lastValue = value;
    }

    @Override
    public long predict() {
        return table[lastHash] + lastValue;
    }
}
