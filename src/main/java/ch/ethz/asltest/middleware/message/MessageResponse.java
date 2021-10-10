package ch.ethz.asltest.middleware.message;

public abstract class MessageResponse extends Message {
    protected MessageResponse(byte[] bytes) {
        super(bytes);
    }

    public abstract boolean isMultiGet();
}
