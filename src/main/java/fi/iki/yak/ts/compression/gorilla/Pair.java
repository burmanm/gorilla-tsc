package fi.iki.yak.ts.compression.gorilla;

/**
 * Created by michael on 8/5/16.
 */
public class Pair {
    private long timestamp;
    private long value;

    public Pair(long timestamp, long value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getDoubleValue() {
        return Double.longBitsToDouble(value);
    }

    public long getLongValue() {
        return value;
    }
}
