package ch.ethz.asltest.middleware.message;

import ch.ethz.asltest.middleware.internal.ProtocolConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessageSetRequest extends MessageRequest {
    private static final Logger logger = LogManager.getLogger(MessageSetRequest.class);

    private final String key;
    private final int flags;
    private final long exptime;

    private MessageSetRequest(byte[] bytes, String key, int flags, long exptime) {
        super(bytes);
        this.key = key;
        this.flags = flags;
        this.exptime = exptime;
    }

    /**
     * Try to parse the message as a MessageSetRequest
     *
     * @param bytes the bytes array
     * @param str   the string corresponding to the byte array
     * @return a parsed MessageSetRequest object or null if there was an error (e.g. the message is of another type)
     */
    protected static MessageSetRequest parse(byte[] bytes, String str) {
        if (str.startsWith(ProtocolConstants.SET_CMD)) {
            if (str.endsWith(ProtocolConstants.NEW_LINE_SEP)) {  //this isn't sufficient for a complete set request but necessary
                String firstLine = str.substring(0, str.indexOf(ProtocolConstants.NEW_LINE_SEP));    //at this point we know at least the first line is present
                String[] els = firstLine.split(ProtocolConstants.ELEMENT_SEP);
                if (els.length < 4) {   //incomplete request
                    return null;
                }

                int dataBlockLength = Integer.parseInt(els[4]) + ProtocolConstants.NEW_LINE_SEP_NO_BYTES; //the data block length including the terminating crlf
                int firstLineLength = firstLine.length() + ProtocolConstants.NEW_LINE_SEP_NO_BYTES; //length of the first line with crlf
                if (firstLineLength + dataBlockLength != bytes.length) { //incomplete set request
                    logger.debug("incomplete set request, received " + bytes.length + " bytes out of " + (firstLineLength + dataBlockLength) + " bytes, returning null");
                    return null;
                }
                int flags = Integer.parseInt(els[2]);
                long expTime = Long.parseLong(els[3]);
                return new MessageSetRequest(bytes, els[1], flags, expTime);
            }
        }
        return null;
    }

    public String getKey() {
        return key;
    }

    public int getFlags() {
        return flags;
    }

    public long getExptime() {
        return exptime;
    }

    @Override
    public boolean isMultiGet() {
        return false;
    }

    @Override
    public String toString() {
        return "MessageSetRequest{" +
                "key='" + key + '\'' +
                ", flags=" + flags +
                ", exptime=" + exptime +
                ", client=" + getClientString() +
                '}';
    }
}
