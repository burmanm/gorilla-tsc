package fi.iki.yak.ts.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;

/**
 * Value decompressor for Gorilla encoded values
 *
 * @author Michael Burman
 */
public class ValueDecompressor {
    private final BitInput in;
    private final Predictor predictor;

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;

    public ValueDecompressor(BitInput input) {
        this(input, new LastValuePredictor());
    }

    public ValueDecompressor(BitInput input, Predictor predictor) {
        this.in = input;
        this.predictor = predictor;
    }

    public long readFirst() {
        long value = in.getLong(Long.SIZE);
        predictor.update(value);
        return value;
    }

    public long nextValue() {
        int val = in.nextClearBit(2);

        switch(val) {
            case 3:
                // New leading and trailing zeros
                storedLeadingZeros = (int) in.getLong(6);

                byte significantBits = (byte) in.getLong(6);
                significantBits++;

                storedTrailingZeros = Long.SIZE - significantBits - storedLeadingZeros;
                // missing break is intentional, we want to overflow to next one
            case 2:
                long value = in.getLong(Long.SIZE - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;

                value = predictor.predict() ^ value;
                predictor.update(value);
                return value;
        }
        return predictor.predict();
    }
}
