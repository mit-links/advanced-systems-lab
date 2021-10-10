package ch.ethz.asltest.middleware.message;

import ch.ethz.asltest.middleware.internal.ProtocolConstants;

import java.util.ArrayList;
import java.util.List;

public class MessageGetRequest extends MessageRequest {
    private final List<String> keys;
    private int serverId = -1;   //the id of the server this request needs to be sent to (only relevant for single gets and non-sharded multigets)


    private MessageGetRequest(byte[] bytes, List<String> keys) {
        super(bytes);
        this.keys = keys;
    }

    /**
     * Try to parse the message as a MessageGetRequest
     *
     * @param bytes the bytes array
     * @param str   the string corresponding to the byte array
     * @return a parsed MessageGetRequest object or null if there was an error (e.g. the message is of another type)
     */
    protected static MessageGetRequest parse(byte[] bytes, String str) {
        if (str.startsWith(ProtocolConstants.GET_CMD) || str.startsWith(ProtocolConstants.GETS_CMD)) {
            if (str.endsWith(ProtocolConstants.NEW_LINE_SEP)) {
                String els[] = str.split(ProtocolConstants.ELEMENT_SEP);
                if (els.length < 2) {   //there should be at least one key along with the cmd
                    return null;
                }
                List<String> keys = new ArrayList<>(els.length - 2);
                for (int i = 1; i < els.length; i++) {
                    keys.add(els[i].trim());    //trim because last key has crlf appended
                }
                return new MessageGetRequest(bytes, keys);
            }
        }
        return null;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getServerId() {
        return serverId;
    }

    @Override
    public boolean isMultiGet() {
        return keys.size() > 1;
    }

    @Override
    public String toString() {
        return "MessageGetRequest{" +
                "keys=" + keys +
                ", serverId=" + serverId +
                ", client=" + getClientString() +
                '}';
    }
}
