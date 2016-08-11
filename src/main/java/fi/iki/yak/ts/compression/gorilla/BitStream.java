package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * Created by michael on 7/29/16.
 */
public class BitStream {
    private ByteBuffer bb;
    private byte b;
    private int bitLeft = Byte.SIZE;
    private int totalBits = 0; // Debug purposes

    public BitStream() {
        bb = ByteBuffer.allocate(1024); // Allocate 1024 bytes .. expand when needed or something
        b = bb.get(0);
    }

    public static void main(String[] args) {
        // TODO Should remove the ByteBuffer handling from the library
    }
}
