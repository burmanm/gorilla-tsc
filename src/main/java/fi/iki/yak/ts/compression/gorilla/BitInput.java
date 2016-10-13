package fi.iki.yak.ts.compression.gorilla;

/**
 * This interface is used for reading a compressed time series.
 *
 * @author Michael Burman
 */
public interface BitInput {

    /**
     * Reads the next bit and returns true if bit is set and false if not.
     *
     * @return true == 1, false == 0
     */
    boolean readBit();

    /**
     * Returns a long that was stored in the next X bits in the stream.
     *
     * @param bits Amount of least significant bits to read from the stream.
     * @return reads the next long in the series using bits meaningful bits
     */
    long getLong(int bits);

    /**
     * Read until next unset bit is found, or until maxBits has been reached.
     *
     * @param maxBits How many bits at maximum until returning
     * @return Integer value of the read bits
     */
    int nextClearBit(int maxBits);
}
