package fi.iki.yak.ts.compression.gorilla;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class IntOutputProto implements BitOutput {
    public static final int DEFAULT_ALLOCATION =  4096*256;

    private int[] intArray;
    private int iB;
    private int position = 0;
    private int bitsLeft = Integer.SIZE;

    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public IntOutputProto() {
        this(DEFAULT_ALLOCATION);
    }

    /**
     * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
     *
     * @param initialSize New initialsize to use
     */
    public IntOutputProto(int initialSize) {
        intArray = new int[initialSize];
        iB = intArray[position];
    }

    private void expandAllocation() {
        int[] largerArray = new int[intArray.length*2];
        System.arraycopy(intArray, 0, largerArray, 0, intArray.length);
        intArray = largerArray;
    }

    private void checkAndFlipByte() {
        if(bitsLeft == 0) {
            flipByte();
        }
    }

    private void flipByte() {
        intArray[position] = iB;
        ++position;
        if(position == (intArray.length - 1)) {
            expandAllocation();
        }
        iB = 0; // Do I need even this?
        bitsLeft = Integer.SIZE;
    }

    /**
     * Sets the next bit (or not) and moves the bit pointer.
     *
     * @param bit true == 1 or false == 0
     */
    public void writeBit(boolean bit) {
        if(bit) {
            iB |= (1 << (bitsLeft - 1));
        }
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
        if(bits > bitsLeft) {
            // First fill the current open byte
            int shift = bits - bitsLeft;
            iB |= ((value >> shift) & ((1 << bitsLeft) - 1)); // should latter be table lookup? Needs testing!
            bits -= bitsLeft;
            flipByte();

            // Then write full bytes
            int loops = (bits / Integer.SIZE);
            for(int j = 0; j < loops; ++j) {
                shift = bits - bitsLeft;
                iB |= ((value) >> shift); // TODO Do I need the AND?
                flipByte();
                bits -= Integer.SIZE;
            }

            // Then the remaining bits
            if(bits > 0) {
                shift = bitsLeft - bits;
                iB |= (value << shift);
                bitsLeft -= bits;
            }

            // No need to do checkAndFlipByte here, we know there's space (otherwise last loop would have been triggered)
        } else {
            int shift = bitsLeft - bits;
            iB |= (value << shift);
            bitsLeft -= bits;
            // No need to do checkAndFlipByte here, we know there's space (otherwise last if would have been triggered)
        }
    }

    /**
     * Causes the currently handled byte to be written to the stream
     */
    @Override
    public void flush() {
        flipByte(); // Causes write to the ByteBuffer
    }

    /**
     * Returns the underlying DirectByteBuffer
     *
     * @return ByteBuffer of type DirectByteBuffer
     */
//    public ByteBuffer getByteBuffer() {
//        return this.bb;
//    }
}
