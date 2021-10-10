package ch.ethz.asltest.middleware.message;

import ch.ethz.asltest.middleware.internal.ProtocolConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MessageGetResponse extends MessageResponse {
    private static final Logger logger = LogManager.getLogger(MessageGetResponse.class);
    private final List<GetResponseValue> getResponseValues;

    private MessageGetResponse(byte[] bytes, List<GetResponseValue> getResponseValues) {
        super(bytes);
        this.getResponseValues = getResponseValues;
    }

    public MessageGetResponse(List<GetResponseValue> getResponseValues) throws IOException {
        super(null);    //set to null, since we will build it later
        this.getResponseValues = getResponseValues;
    }

    /**
     * Try to parse the message as a MessageGetResponse
     *
     * @param bytes the bytes array
     * @param str   the string corresponding to the byte array
     * @return a parsed MessageGetResponse object or null if there was an error (e.g. the message is of another type)
     */
    protected static MessageGetResponse parse(byte[] bytes, String str) {
        String endBlock = ProtocolConstants.END + ProtocolConstants.NEW_LINE_SEP;
        if (str.startsWith(ProtocolConstants.VALUE) || str.startsWith(endBlock)) {
            if (str.endsWith(endBlock)) {  //necessary but not sufficient (might be a value called like this)
                if (str.equals(endBlock)) {
                    return new MessageGetResponse(bytes, new ArrayList<>());   //if the str is only END\r\n, it's an empty response
                }

                int valueLineStart = 0;

                List<GetResponseValue> getResponseValues = new ArrayList<>();
                assert str.length() == bytes.length;

                while (true) {
                    int valueLineEnd = str.indexOf(ProtocolConstants.NEW_LINE_SEP, valueLineStart); //first new line after start of value line
                    if (valueLineEnd == -1) {    //value line incomplete
                        return null;
                    }
                    if (str.substring(valueLineStart).equals(endBlock)) { //end block reached
                        break;
                    }

                    String valueLine = new String(bytes, valueLineStart, valueLineEnd - valueLineStart, StandardCharsets.US_ASCII); //of the form VALUE key flags bytes \r\n
                    String[] els = valueLine.split(ProtocolConstants.ELEMENT_SEP);

                    if (els.length < 3) {   //incomplete request
                        return null;
                    }
                    int dataBlockLength;
                    int flags;
                    try {
                        dataBlockLength = Integer.parseInt(els[3]); //third element is data block length
                        flags = Integer.parseInt(els[2]);
                    } catch (NumberFormatException e) {
                        logger.info("parsing incomplete request threw number format exception, nothing to worry about", e);
                        return null;
                    }

                    String key = els[1];
                    int newValueLineStart = valueLineEnd + dataBlockLength + 2 * ProtocolConstants.NEW_LINE_SEP_NO_BYTES;   //index of the start of the next value or end block
                    GetResponseValue getResponseValue = new GetResponseValue(key, flags, Arrays.copyOfRange(bytes, valueLineStart, newValueLineStart));
                    getResponseValues.add(getResponseValue);
                    valueLineStart = newValueLineStart;  //skip the data block

                }
                return new MessageGetResponse(bytes, getResponseValues);
            }
        }
        return null;
    }

    public List<GetResponseValue> getGetResponseValues() {
        return Collections.unmodifiableList(getResponseValues); //return an unmodifiable list because otherwise the byte array and the list might get out of sync
    }

    @Override
    public boolean isMultiGet() {
        return getResponseValues.size() > 1;
    }

    @Override
    public byte[] getBytes() {
        if (bytes != null) {    //bytes have been set already earlier
            return bytes;
        }

        //build the byte array on the fly
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            for (GetResponseValue getResponseValue : getResponseValues) {
                stream.write(getResponseValue.getBytes());
            }
            stream.write((ProtocolConstants.END + ProtocolConstants.NEW_LINE_SEP).getBytes());  //add terminating end block
            this.setBytes(stream.toByteArray());
            return bytes;
        } catch (IOException e) {
            logger.error("exception occurede while getting bytes for get response, returning null", e);
            return null;
        }
    }

    @Override
    public String toString() {
        return "MessageGetResponse{" +
                "getResponseValues=" + getResponseValues +
                ", client=" + getClientString() +
                '}';
    }
}
