package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class LongOutputProto implements BitOutput2 {
    public static final int DEFAULT_ALLOCATION =  4096*32;

    private long[] longArray;
    private long lB;
    private int position = 0;
    private int bitsLeft = Long.SIZE;

    public final static long[] MASK_ARRAY;
    public final static long[] BIT_SET_MASK;

    // Java does not allow creating 64 bit masks with (1L << 64) - 1; (end result is 0)
    // TODO Mention in the thesis (problems in the Java land)
    static {
        MASK_ARRAY = new long[64];
        long mask = 1;
        long value = 0;
        for (int i = 0; i < MASK_ARRAY.length; i++) {
            value = value | mask;
            mask = mask << 1;

            MASK_ARRAY[i] = value;
        }

        BIT_SET_MASK = new long[64];
        for(int i = 0; i < BIT_SET_MASK.length; i++) {
            BIT_SET_MASK[i] = (1L << i);
        }
    }


    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public LongOutputProto() {
        this(DEFAULT_ALLOCATION);
    }

    /**
     * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
     *
     * @param initialSize New initialsize to use
     */
    public LongOutputProto(int initialSize) {
        longArray = new long[initialSize];
        lB = longArray[position];
    }

    private void expandAllocation() {
        long[] largerArray = new long[longArray.length*2];
        System.arraycopy(longArray, 0, largerArray, 0, longArray.length);
        longArray = largerArray;
    }

    private void checkAndFlipByte() {
        // Wish I could avoid this check in most cases...
        if(bitsLeft == 0) {
            flipByte();
        }
    }

    private void flipByte() {
        longArray[position] = lB;
        ++position;
        if(position >= (longArray.length - 2)) { // We want to have always at least 2 longs available
            expandAllocation();
        }
        lB = 0;
        bitsLeft = Long.SIZE;
    }

    private void flipByteWithoutExpandCheck() {
        longArray[position] = lB;
        ++position;
        lB = 0; // Do I need even this?
        bitsLeft = Long.SIZE;
    }

    /**
     * Sets the next bit (or not) and moves the bit pointer.
     */
    public void writeBit() {
        lB |= BIT_SET_MASK[bitsLeft - 1];
        bitsLeft--;
        checkAndFlipByte();
    }

    public void skipBit() {
        bitsLeft--;
        checkAndFlipByte();
    }

    /**
     * Writes the given long to the stream using bits amount of meaningful bits. This command does not
     * check input values, so if they're larger than what can fit the bits (you should check this before writing),
     * expect some weird results.
     *
     * @param value Value to be written to the stream
     * @param bits How many bits are stored to the stream
     */
    public void writeBits(long value, int bits) {
        if(bits <= bitsLeft) {
            int lastBitPosition = bitsLeft - bits;
            lB |= (value << lastBitPosition) & MASK_ARRAY[bitsLeft - 1];
            bitsLeft -= bits;
            checkAndFlipByte(); // We could be at 0 bits left because of the <= condition .. would it be faster with
                                // the other one?
        } else {
            value &= MASK_ARRAY[bits - 1];
            int firstBitPosition = bits - bitsLeft;
            lB |= value >>> firstBitPosition;
            bits -= bitsLeft;
            flipByteWithoutExpandCheck();
            lB |= value << (64 - bits);
            bitsLeft -= bits;
        }
    }

    /**
     * Causes the currently handled byte to be written to the stream
     */
    @Override
    public void flush() {
        flipByte(); // Causes write to the ByteBuffer
    }

    public void reset() {
        position = 0;
        bitsLeft = Long.SIZE;
        lB = 0;
    }

    /**
     * Returns the underlying DirectByteBuffer
     *
     * @return ByteBuffer of type DirectByteBuffer
     */
    public ByteBuffer getByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(position * 8);
        bb.asLongBuffer().put(longArray, 0, position);
        bb.position(position * 8);
        return bb;
    }

    public long[] getLongArray() {
        long[] copy = new long[position+1];
        System.arraycopy(longArray, 0, copy, 0, position);
        return copy;
    }
}
