package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * @author Michael Burman
 */
public class ByteBufferBitInput implements BitInput {
    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = 0;

    public ByteBufferBitInput(ByteBuffer buf) {
        bb = buf;
        flipByte();
    }

    public ByteBufferBitInput(byte[] input) {
        this(ByteBuffer.wrap(input));
    }

    public boolean readBit() {
//        byte bit = (byte) ((b >> (bitsLeft - 1)) & 1);
        boolean bit = ((b >> (bitsLeft - 1)) & 1) == 1;
        bitsLeft--;
        flipByte();
        return bit;
//        return bit == 1;
    }

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
                // TODO This isn't optimal - we need a range of bits
                for(; bits > 0; bits--) {
                    byte bit = (byte) ((b >> (bitsLeft - 1)) & 1);
                    bitsLeft--;
                    value = (value << 1) + (bit & 0xFF);
                }
            }
            flipByte();
        }
        return value;
    }

    private void flipByte() {
        if (bitsLeft == 0) {
            b = bb.get();
            bitsLeft = Byte.SIZE;
        }
    }

    public ByteBuffer getByteBuffer() {
        return this.bb;
    }
}
