package ch.ethz.asltest.middleware.message;

public abstract class MessageRequest extends Message {
    private long id;
    private long msgInTs;
    private long queueInTs;
    private long queueOutTs;
    private long toServerTs;
    private long fromServerTs;
    private long msgOutTs;

    protected MessageRequest(byte[] bytes) {
        super(bytes);
    }

    public abstract boolean isMultiGet();

    public long getId() {
        return id;
    }


    public void setId(long id) {
        this.id = id;
    }


    public long getMsgInTs() {
        return msgInTs;
    }


    public void setMsgInTs(long msgInTs) {
        this.msgInTs = msgInTs;
    }


    public long getQueueInTs() {
        return queueInTs;
    }


    public void setQueueInTs(long queueInTs) {
        this.queueInTs = queueInTs;
    }


    public long getQueueOutTs() {
        return queueOutTs;
    }


    public void setQueueOutTs(long queueOutTs) {
        this.queueOutTs = queueOutTs;
    }


    public long getToServerTs() {
        return toServerTs;
    }


    public void setToServerTs(long toServerTs) {
        this.toServerTs = toServerTs;
    }


    public long getFromServerTs() {
        return fromServerTs;
    }


    public void setFromServerTs(long fromServerTs) {
        this.fromServerTs = fromServerTs;
    }


    public long getMsgOutTs() {
        return msgOutTs;
    }


    public void setMsgOutTs(long msgOutTs) {
        this.msgOutTs = msgOutTs;
    }
}
