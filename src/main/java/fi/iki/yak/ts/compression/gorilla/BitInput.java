package fi.iki.yak.ts.compression.gorilla;

/**
 * @author Michael Burman
 */
public interface BitInput {
    boolean readBit();
    long getLong(int bits);
}
