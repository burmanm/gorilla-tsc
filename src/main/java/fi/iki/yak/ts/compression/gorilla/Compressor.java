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

    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = Byte.SIZE;
    private int totalBits = 0;

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

            writeBits(storedDelta, 14);
            writeBits(Double.doubleToRawLongBits(storedVal), 64);
            System.out.println("First value was: timestamp->" + storedTimestamp + ", val->" + storedVal + ", delta->" + storedDelta);
        } else {
            compressTimestamp(timestamp);
            compressValue(value);
            System.out.println("Next value was: timestamp->" + storedTimestamp + ", val->" + storedVal + ", delta->" + storedDelta);
        }
    }

    public void Close() {
        // Close the bytebuffers.. and write something so we know next time it's done?

        // These are selected to test interoperability and correctness of the solution, this can be read with go-tsz
        writeBits(0x0F, 4);
        writeBits(0xFFFFFFFF, 32);
        writeBit(false);
        System.out.printf("Wrote a total of %d bits, which is about %d bytes\n", totalBits, (totalBits / 8));
        System.out.printf("BB: pos->%d, cap->%d\n", bb.position(), bb.capacity());
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
        long delta = (timestamp - storedTimestamp) - storedDelta;

        // If delta is zero, write single 0 bit
        if(delta == 0) {
            writeBit(false);
        } else if(delta >= -63 && delta <= 64) {
            writeBits(0x02, 2); // store '10'
            writeBits(delta, 7); // Using 7 bits, store the value..
        } else if(delta >= 255 && delta <= 256) {
            writeBits(0x06, 3); // store '110'
            writeBits(delta, 9); // Use 9 bits
        } else if(delta >= 2047 && delta <= 2048) {
            writeBits(0x0E, 4); // store '1110'
            writeBits(delta, 12); // Use 12 bits
        } else {
            writeBits(0x0F, 4); // Store '1111'
            writeBits(delta, 32); // Store delta using 32 bits
        }

        storedDelta = delta;
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

            // Store bit '1'
            writeBit(true);

            if(leadingZeros != Integer.MAX_VALUE && leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                writeBit(false);
                // If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
                // + store the meaningful XORed value
                int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                writeBits(xor >> trailingZeros, significantBits);
            } else {
                // store the length of the number of leading zeros in the next 5 bits
                // + store length of the meaningful XORed value in the next 6 bits,
                // + store the meaningful bits of the XORed value
                // (type b)
                writeBit(true);
                writeBits(leadingZeros, 5); // Number of leading zeros in the next 5 bits

                int significantBits = 64 - leadingZeros - trailingZeros;
                writeBits(significantBits, 6); // Length of meaningful bits in the next 6 bits
                writeBits(xor >> trailingZeros, significantBits); // Store the meaningful bits of XOR

                storedTrailingZeros = leadingZeros;
                storedTrailingZeros = trailingZeros;
            }
        }

        storedVal = value;
    }

    private void flipByte() {
        if(bitsLeft == 0) {
//            System.out.printf("Writing b-> %8s\n", Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
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
        totalBits++; // debug only
        if(bit) {
            b |= (1 << (bitsLeft - 1));
        }
        bitsLeft--;
//        System.out.printf("Wrote %b, bitsLeft: %d\n", bit, bitsLeft);
        flipByte();
    }


    private void writeBits(long value, int bits) {
        totalBits += bits;
        int remaining = bits; // Unnecessary, just use bits

//        System.out.printf("Requested to write value->%d, bits->%d", value, bits);

//        System.out.printf("Writing orig->%d, unsigned->%s\n", value, Long.toUnsignedString(value));
//        System.out.printf("Write LongValue->%64s\n", Long.toBinaryString(value));

        while(remaining > 0) {
            int shift = remaining - bitsLeft;
            if(remaining > bitsLeft) {
                remaining -= bitsLeft;
                bitsLeft = 0;
            } else {
                bitsLeft -= remaining;
                remaining = 0;
            }
            // TODO Should I optimize the 0 shift case?
            if(shift > 0) {
                b |= (byte) (value >> shift);
//                System.out.printf("writeBits positive, shifted->%d, b-> %8s\n", shift, Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
//                b |= (byte) ((value >> shift) & ((1<<8)-1));
            } else {
                int shiftAmount = Math.abs(shift);
                b |= (byte) (value << shiftAmount);
//                System.out.printf("writeBits negative, shifted->%d, b-> %8s\n", shiftAmount, Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
//                b |= (byte) ((value << shiftAmount) & ((1<<8)-1));
            }
            flipByte();
        }
    }
}
