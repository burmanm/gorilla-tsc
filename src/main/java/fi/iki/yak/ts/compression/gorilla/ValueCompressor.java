package fi.iki.yak.ts.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;

/**
 * ValueCompressor for the Gorilla encoding format. Supply with long presentation of the value,
 * in case of doubles use Double.doubleToRawLongBits(value)
 *
 * @author Michael Burman
 */
public class ValueCompressor {
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;

    private Predictor predictor;
    private BitOutput out;

    public ValueCompressor(BitOutput out) {
        this(out, new LastValuePredictor());
    }

    public ValueCompressor(BitOutput out, Predictor predictor) {
        this.out = out;
        this.predictor = predictor;
    }

    void writeFirst(long value) {
        predictor.update(value);
        out.writeBits(value, 64);
    }

    protected void compressValue(long value) {
        // In original Gorilla, Last-Value predictor is used
        long diff = predictor.predict() ^ value;
        predictor.update(value);

        if(diff == 0) {
            // Write 0
            out.skipBit();
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(diff);
            int trailingZeros = Long.numberOfTrailingZeros(diff);

            out.writeBit(); // Optimize to writeNewLeading / writeExistingLeading?

            if(leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                writeExistingLeading(diff);
            } else {
                writeNewLeading(diff, leadingZeros, trailingZeros);
            }
        }
    }

    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
     * store the meaningful XORed value
     *
     * @param xor XOR between previous value and current
     */
    private void writeExistingLeading(long xor) {
        out.skipBit();

        int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
        xor >>>= storedTrailingZeros;
        out.writeBits(xor, significantBits);
    }

    /**
     * store the length of the number of leading zeros in the next 5 bits
     * store length of the meaningful XORed value in the next 6 bits,
     * store the meaningful bits of the XORed value
     * (type b)
     *
     * @param xor XOR between previous value and current
     * @param leadingZeros New leading zeros
     * @param trailingZeros New trailing zeros
     */
    private void writeNewLeading(long xor, int leadingZeros, int trailingZeros) {
        out.writeBit();

        // Different from version 1.x, use (significantBits - 1) in storage - avoids a branch
        int significantBits = 64 - leadingZeros - trailingZeros;

        // Different from original, bits 5 -> 6, avoids a branch, allows storing small longs
        out.writeBits(leadingZeros, 6); // Number of leading zeros in the next 6 bits
        out.writeBits(significantBits - 1, 6); // Length of meaningful bits in the next 6 bits
        out.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

        storedLeadingZeros = leadingZeros;
        storedTrailingZeros = trailingZeros;
    }
}
