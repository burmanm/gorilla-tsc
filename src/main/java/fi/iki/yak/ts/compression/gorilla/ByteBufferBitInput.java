package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * An implementation of BitInput that parses the data from byte array or existing ByteBuffer.
 *
 * @author Michael Burman
 */
public class ByteBufferBitInput implements BitInput {
    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = 0;

    /**
     * Uses an existing ByteBuffer to read the stream. Starts at the ByteBuffer's current position.
     *
     * @param buf Use existing ByteBuffer
     */
    public ByteBufferBitInput(ByteBuffer buf) {
        bb = buf;
        flipByte();
    }

    public ByteBufferBitInput(byte[] input) {
        this(ByteBuffer.wrap(input));
    }

    /**
     * Reads the next bit and returns a boolean representing it.
     *
     * @return true if the next bit is 1, otherwise 0.
     */
    public boolean readBit() {
        boolean bit = ((b >> (bitsLeft - 1)) & 1) == 1;
        bitsLeft--;
        flipByte();
        return bit;
    }

    /**
     * Reads a long from the next X bits that represent the least significant bits in the long value.
     *
     * @param bits How many next bits are read from the stream
     * @return long value that was read from the stream
     */
    public long getLong(int bits) {
        long value = 0;
        while(bits > 0) {
            if(bits > bitsLeft || bits == Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                byte d = (byte) (b & ((1<<bitsLeft) - 1));
                value = (value << bitsLeft) + (d & 0xFF);
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                // Shift to correct position and take only least significant bits
                byte d = (byte) ((b >>> (bitsLeft - bits)) & ((1<<bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte();
        }
        return value;
    }

    @Override
    public int nextClearBit(int maxBits) {
        int val = 0x00;

        for(int i = 0; i < maxBits; i++) {
            val <<= 1;
            boolean bit = readBit();

            if(bit) {
                val |= 0x01;
            } else {
                break;
            }
        }
        return val;
    }

    private void flipByte() {
        if (bitsLeft == 0) {
            b = bb.get();
            bitsLeft = Byte.SIZE;
        }
    }

    /**
     * Returns the underlying ByteBuffer
     *
     * @return ByteBuffer that's connected to the underlying stream
     */
    public ByteBuffer getByteBuffer() {
        return this.bb;
    }
}
