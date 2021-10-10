package ch.ethz.asltest.middleware.message;

import ch.ethz.asltest.middleware.internal.ProtocolConstants;

public class MessageError extends MessageResponse {
    private final String errorString;

    private MessageError(byte[] bytes, String errorString) {
        super(bytes);
        this.errorString = errorString;
    }

    /**
     * Try to parse the message as a MessageError
     *
     * @param bytes the bytes array
     * @param str   the string corresponding to the byte array
     * @return a parsed MessageError object or null if there was an error (e.g. the message is of another type)
     */
    protected static MessageError parse(byte[] bytes, String str) {
        if (str.startsWith(ProtocolConstants.ERROR) || str.startsWith(ProtocolConstants.CLIENT_ERROR) || str.startsWith(ProtocolConstants.SERVER_ERROR)) {
            if (str.endsWith(ProtocolConstants.NEW_LINE_SEP)) {
                return new MessageError(bytes, str.trim());
            }
        }
        return null;
    }

    public String getErrorString() {
        return errorString;
    }

    @Override
    public String toString() {
        return "MessageError{" +
                "errorString='" + errorString + '\'' +
                ", client=" + getClientString() +
                '}';
    }

    @Override
    public boolean isMultiGet() {
        return false;
    }
}
