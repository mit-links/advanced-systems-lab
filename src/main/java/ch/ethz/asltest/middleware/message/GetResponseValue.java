package ch.ethz.asltest.middleware.message;

public class GetResponseValue {
    private final String key;
    private final int flags;
    private final byte[] bytes; //bytes from VALUE until and including the terminating crlf of the data block

    GetResponseValue(String key, int flags, byte[] bytes) {
        this.key = key;
        this.flags = flags;
        this.bytes = bytes;
    }

    public String getKey() {
        return key;
    }

    public int getFlags() {
        return flags;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "GetResponseValue{" +
                "key='" + key + '\'' +
                ", flags=" + flags +
                '}';
    }
}
