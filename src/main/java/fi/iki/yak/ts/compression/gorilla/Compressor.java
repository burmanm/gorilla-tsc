package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

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

    public final static int FIRST_DELTA_BITS = 27;

    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = Byte.SIZE;

    // We should have access to the series?

    public Compressor(long timestamp) {
        blockTimestamp = timestamp;
        bb = ByteBuffer.allocate(1024); // Allocate 1024 bytes .. expand when needed or something
        b = bb.get(0);
        addHeader(timestamp);
    }

    // The requested block size -> necessary for the first delta

    // We need: requested timestamp precision (1s precision gives better compression)
    // -> include precision information to a header
    // What happens if we have two values at the same timestamp?

    // Precisions? 1 = 1s, 1000 = ms? ENUM for that..?

    // Add header here..?

    // TODO Investigate another compression for millisecond precision? Delta-deltas are never 0 for these..
    // JavaFastPFOR for example? https://github.com/lemire/JavaFastPFOR

    // Should we store the sample count as last one? Helps in decompression (allocate only an array of size X)

    // TODO Keep this clean Gorilla and then allow plugging just timestamp compressing or value compressor from this class

    // TODO I need an int64 version of this also.. we're not always compressing doubles

    // TODO Split timestamp compression & value compression, might want to use different timestamp compression for example

    private void addHeader(long timestamp) {
        // One byte: length of the first delta
        // One byte: precision of timestamps
        writeBits(timestamp, 64);
    }

    public void addValue(long timestamp, double value) {
        // TODO If given timestamp is out of the block boundary, return error! Or just return new instance? ;)
        if(storedTimestamp == 0) {
            storedDelta = timestamp - blockTimestamp;
            storedTimestamp = timestamp;
            storedVal = value;

            writeBits(storedDelta, FIRST_DELTA_BITS);
            writeBits(Double.doubleToRawLongBits(storedVal), 64);
        } else {
            compressTimestamp(timestamp);
            compressValue(value);
        }
    }

    public void close() {
        // close the bytebuffers.. and write something so we know next time it's done?

        // These are selected to test interoperability and correctness of the solution, this can be read with go-tsz
        writeBits(0x0F, 4);
        writeBits(0xFFFFFFFF, 32);
        writeBit(false);
    }

    public ByteBuffer getByteBuffer() {
        return this.bb;
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
            writeBit(false);
        } else if(deltaD >= -63 && deltaD <= 64) {
            writeBits(0x02, 2); // store '10'
            writeBits(deltaD, 7); // Using 7 bits, store the value..
        } else if(deltaD >= -255 && deltaD <= 256) {
            writeBits(0x06, 3); // store '110'
            writeBits(deltaD, 9); // Use 9 bits
        } else if(deltaD >= -2047 && deltaD <= 2048) {
            writeBits(0x0E, 4); // store '1110'
            writeBits(deltaD, 12); // Use 12 bits
        } else {
            writeBits(0x0F, 4); // Store '1111'
            writeBits(deltaD, 32); // Store delta using 32 bits
        }

        storedDelta = newDelta;
        storedTimestamp = timestamp;
    }

    private void compressValue(double value) {
        long xor = Double.doubleToRawLongBits(storedVal) ^ Double.doubleToRawLongBits(value);

        if(xor == 0) {
            // Write 0
            writeBit(false);
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);

            // Check overflow of leading? Can't be 32!

            if(leadingZeros >= 32) {
                leadingZeros = 31;
            }

            // Store bit '1'
            writeBit(true);

            // This should be >= for these checks, need to fix later (there's a bug if you just change them)
            if(leadingZeros != Integer.MAX_VALUE && leadingZeros == storedLeadingZeros && trailingZeros == storedTrailingZeros) {
                writeBit(false);
                // If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
                // + store the meaningful XORed value
                int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                writeBits(xor >>> trailingZeros, significantBits);
            } else {
                // store the length of the number of leading zeros in the next 5 bits
                // + store length of the meaningful XORed value in the next 6 bits,
                // + store the meaningful bits of the XORed value
                // (type b)
                writeBit(true);
                writeBits(leadingZeros, 5); // Number of leading zeros in the next 5 bits

                int significantBits = 64 - leadingZeros - trailingZeros;
                writeBits(significantBits, 6); // Length of meaningful bits in the next 6 bits
                writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

                storedLeadingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
            }
        }

        storedVal = value;
    }

    private void flipByte() {
        if(bitsLeft == 0) {
            bb.put(b);
            if(!bb.hasRemaining()) {
                // TODO We need a new allocation
                throw new RuntimeException("Temporarily fail for testing purposes");
            } else {
                b = bb.get(bb.position());
                bitsLeft = Byte.SIZE;
            }
        }
    }

    private void writeBit(boolean bit) { // Why int here..? Something else perhaps? boolean stinks too :(
        if(bit) {
            b |= (1 << (bitsLeft - 1));
        }
        bitsLeft--;
        flipByte();
    }


    private void writeBits(long value, int bits) {
        while(bits > 0) {
            int shift = bits - bitsLeft;
            // TODO Should I optimize the 0 shift case?
            if(shift > 0) {
                b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
            } else {
                int shiftAmount = Math.abs(shift);
                b |= (byte) (value << shiftAmount);
            }
            if(bits > bitsLeft) {
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte();
        }
    }
}
