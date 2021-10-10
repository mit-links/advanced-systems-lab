package ch.ethz.asltest.middleware.message;

import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * Parses an incoming request in the form of a byte array and returns a Message object.
 */
public class MessageParser {
    /**
     * Given a byte array, try to parse it as a message type.
     *
     * @param bytes  the provided byte array from the message
     * @param client the client socket
     * @return a Message object or null if there was an error or the message is incomplete resp.
     */
    public static Message parse(byte[] bytes, SocketChannel client) {
        Message msg = parse(bytes);
        if (msg != null) {
            msg.setClient(client);
        }
        return msg;
    }

    /**
     * Given a byte array, try to parse it as a message type.
     *
     * @param bytes the provided byte array from the message
     * @return a Message object or null if there was an error or the message is incomplete resp.
     */
    private static Message parse(byte[] bytes) {
        Message msg;

        String str = new String(bytes, StandardCharsets.US_ASCII); //potential data blocks in this string won't make sense but we need it to find out which type of query it is
        msg = MessageError.parse(bytes, str);
        if (msg != null) {
            return msg;
        }
        msg = MessageGetRequest.parse(bytes, str);
        if (msg != null) {
            return msg;
        }
        msg = MessageGetResponse.parse(bytes, str);
        if (msg != null) {
            return msg;
        }
        msg = MessageSetRequest.parse(bytes, str);
        if (msg != null) {
            return msg;
        }
        msg = MessageSetResponse.parse(bytes, str);
        if (msg != null) {
            return msg;
        }
        return null;

    }
}
