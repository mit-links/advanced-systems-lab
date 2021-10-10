package ch.ethz.asltest.middleware.networking;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

/**
 * Contains some helpers methods for communicating with servers/clients.
 */
public class IOHelpers {
    /**
     * Send a request to multiple servers
     *
     * @param servers the list of servers where the request should be sent to
     * @param buffer  the buffer ready for reading
     * @throws IOException
     */
    public static void sendRequestToServers(List<SocketChannel> servers, ByteBuffer buffer) throws IOException {
        for (SocketChannel server : servers) {
            server.write(buffer);
            buffer.rewind();    //rewind buffer for re-reading
        }
        buffer.clear();
    }

    /**
     * Send a request to a single server
     *
     * @param server the server where the request should be sent to
     * @param buffer the buffer ready for reading
     * @throws IOException
     */
    public static void sendRequestToServer(SocketChannel server, ByteBuffer buffer) throws IOException {
        server.write(buffer);
        buffer.clear();
    }

    /**
     * Read a response from multiple server
     *
     * @param servers a list of servers
     * @param buffer  the buffer for reading
     * @return an array of ByteArrayOutputStreams containing the data read
     * @throws IOException
     */
    public static ByteArrayOutputStream[] getResponseFromServers(List<SocketChannel> servers, ByteBuffer buffer) throws IOException {
        ByteArrayOutputStream[] ret = new ByteArrayOutputStream[servers.size()];
        for (int i = 0; i < servers.size(); i++) {
            SocketChannel server = servers.get(i);
            ret[i] = getResponseFromServer(server, buffer);
        }
        return ret;
    }

    /**
     * Read a response from a server
     *
     * @param server the server
     * @param buffer the buffer for reading
     * @return a ByteArrayOutputStream containing the read data
     * @throws IOException
     */
    public static ByteArrayOutputStream getResponseFromServer(SocketChannel server, ByteBuffer buffer) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        buffer.clear();
        int l = server.read(buffer);
        buffer.clear();    //set position to 0
        byte[] bufArr = new byte[l];
        buffer.get(bufArr);    //write buffer to array
        outputStream.write(bufArr); //write array to byte stream
        buffer.clear();    //set position to 0

        return outputStream;
    }
}
