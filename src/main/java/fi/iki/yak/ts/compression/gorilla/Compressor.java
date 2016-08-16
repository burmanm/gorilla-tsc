package fi.iki.yak.ts.compression.gorilla;

/**
 * Implements the floating point compression as described in the Facebook's Gorilla Paper.. Heavily inspired by
 * the go-tsz package
 *
 * @author Michael Burman
 */
public class Compressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private double storedVal = 0;
    private long storedTimestamp = 0;
    private long storedDelta = 0;

    private long blockTimestamp = 0;

    public final static short FIRST_DELTA_BITS = 27;

    private BitOutput out;

    // We should have access to the series?
    public Compressor(long timestamp, BitOutput output) {
        blockTimestamp = timestamp;
        out = output;
        addHeader(timestamp);
    }

    private void addHeader(long timestamp) {
        // One byte: length of the first delta
        // One byte: precision of timestamps
        out.writeBits(timestamp, 64);
    }

    public void addValue(long timestamp, double value) {
        // TODO If given timestamp is out of the block boundary, return error! Or just return new instance? ;)
        if(storedTimestamp == 0) {
            storedDelta = timestamp - blockTimestamp;
            storedTimestamp = timestamp;
            storedVal = value;

            out.writeBits(storedDelta, FIRST_DELTA_BITS);
            out.writeBits(Double.doubleToRawLongBits(storedVal), 64);
        } else {
            compressTimestamp(timestamp);
            compressValue(value);
        }
    }

    public void close() {
        // These are selected to test interoperability and correctness of the solution, this can be read with go-tsz
        out.writeBits(0x0F, 4);
        out.writeBits(0xFFFFFFFF, 32);
        out.writeBit(false);
        out.flush();
    }

    /**
     * Difference to the original Facebook paper, we store the first delta as 27 bits to allow
     * millisecond accuracy for a one day block.
     *
     * Also, the timestamp delta-delta is not good for millisecond compressions.. might make more
     * sense to always assume larger number (like 9 bits as the smallest number, 12, 15 or something? - needs
     * testing)
     *
     * @param timestamp
     */
    private void compressTimestamp(long timestamp) {
        // a) Calculate the delta of delta
        long newDelta = (timestamp - storedTimestamp);
        long deltaD = newDelta - storedDelta;

        // If delta is zero, write single 0 bit
        if(deltaD == 0) {
            out.writeBit(false);
        } else if(deltaD >= -63 && deltaD <= 64) {
            out.writeBits(0x02, 2); // store '10'
            out.writeBits(deltaD, 7); // Using 7 bits, store the value..
        } else if(deltaD >= -255 && deltaD <= 256) {
            out.writeBits(0x06, 3); // store '110'
            out.writeBits(deltaD, 9); // Use 9 bits
        } else if(deltaD >= -2047 && deltaD <= 2048) {
            out.writeBits(0x0E, 4); // store '1110'
            out.writeBits(deltaD, 12); // Use 12 bits
        } else {
            out.writeBits(0x0F, 4); // Store '1111'
            out.writeBits(deltaD, 32); // Store delta using 32 bits
        }

        storedDelta = newDelta;
        storedTimestamp = timestamp;
    }

    private void compressValue(double value) {
        // TODO Fix already compiled into a big method
        long xor = Double.doubleToRawLongBits(storedVal) ^ Double.doubleToRawLongBits(value);

        if(xor == 0) {
            // Write 0
            out.writeBit(false);
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);

            // Check overflow of leading? Can't be 32!
            if(leadingZeros >= 32) {
                leadingZeros = 31;
            }

            // Store bit '1'
            out.writeBit(true);

            // This should be >= for these checks, need to fix later (there's a bug if you just change them)
            if(leadingZeros != Integer.MAX_VALUE && leadingZeros == storedLeadingZeros && trailingZeros == storedTrailingZeros) {
                out.writeBit(false);
                // If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
                // + store the meaningful XORed value
                int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                out.writeBits(xor >>> trailingZeros, significantBits);
            } else {
                // store the length of the number of leading zeros in the next 5 bits
                // + store length of the meaningful XORed value in the next 6 bits,
                // + store the meaningful bits of the XORed value
                // (type b)
                out.writeBit(true);
                out.writeBits(leadingZeros, 5); // Number of leading zeros in the next 5 bits

                int significantBits = 64 - leadingZeros - trailingZeros;
                out.writeBits(significantBits, 6); // Length of meaningful bits in the next 6 bits
                out.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

                storedLeadingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
            }
        }

        storedVal = value;
    }
}
