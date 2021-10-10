package ch.ethz.asltest.middleware.message;

import ch.ethz.asltest.middleware.internal.ProtocolConstants;

public class MessageSetResponse extends MessageResponse {
    private final String message;

    private MessageSetResponse(byte[] bytes, String message) {
        super(bytes);
        this.message = message;
    }

    /**
     * Try to parse the message as a MessageSetResponse
     *
     * @param bytes the bytes array
     * @param str   the string corresponding to the byte array
     * @return a parsed MessageSetResponse object or null if there was an error (e.g. the message is of another type)
     */
    protected static MessageSetResponse parse(byte[] bytes, String str) {
        if (str.startsWith(ProtocolConstants.STORED) || str.startsWith(ProtocolConstants.NOT_STORED) ||
                str.startsWith(ProtocolConstants.EXISTS) || str.startsWith(ProtocolConstants.NOT_FOUND)) {
            if (str.endsWith(ProtocolConstants.NEW_LINE_SEP)) {
                return new MessageSetResponse(bytes, str.trim());
            }
        }
        return null;
    }

    public String getMessage() {
        return message;
    }

    public boolean isSuccess() {
        return message.equals(ProtocolConstants.STORED);
    }

    @Override
    public boolean isMultiGet() {
        return false;
    }

    @Override
    public String toString() {
        return "MessageSetResponse{" +
                "message='" + message + '\'' +
                ", client=" + getClientString() +
                '}';
    }
}
