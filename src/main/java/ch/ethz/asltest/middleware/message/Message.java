package ch.ethz.asltest.middleware.message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Holds all information about a request. This includes the byte array making up the request and the client socket.
 * Subclasses can add additional fields for easier handling (like a key for a set request)
 */
public abstract class Message {
    private static final Logger logger = LogManager.getLogger(Message.class);
    protected byte[] bytes;
    protected SocketChannel client;

    protected Message(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    protected void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public SocketChannel getClient() {
        return client;
    }

    public void setClient(SocketChannel client) {
        this.client = client;
    }

    /**
     * Get the client address as a string.
     *
     * @return the client address or null if there is an error or the client is null
     */
    protected String getClientString() {
        try {
            if (client != null) {
                return client.getRemoteAddress().toString();
            }
            return null;
        } catch (IOException e) {
            logger.debug("couldn't get client address for debugging purposes", e);
            return null;
        }
    }


}
