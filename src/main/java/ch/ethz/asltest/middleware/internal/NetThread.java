package ch.ethz.asltest.middleware.internal;


import ch.ethz.asltest.middleware.logging.StatsLogger;
import ch.ethz.asltest.middleware.message.MessageGetRequest;
import ch.ethz.asltest.middleware.message.MessageParser;
import ch.ethz.asltest.middleware.message.MessageRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The net thread which listens to incoming requests from clients, parses them and puts them in the request queue.
 */
public class NetThread extends Thread {
    private static final Logger logger = LogManager.getLogger(NetThread.class);
    private static final String NEW_LINE = "\n";

    private final String ip;
    private final int port;
    private final boolean log;
    private final int noServers;
    private final boolean sharded;
    private final StatsLogger statsLogger;
    private long idCounter = 0L;    //counter for ids
    private int roundRobinCounter = 0;    //counter for round robin for gets
    private ServerSocketChannel serverChannel;
    private Selector selector;
    private Map<SocketChannel, ClientInfo> clients = new HashMap<>();
    private boolean firstClientConnected = false;

    public NetThread(String ip, int port, boolean log, int numberOfServers, boolean sharded, StatsLogger statsLogger) throws IOException {
        this.ip = ip;
        this.port = port;
        this.log = log;
        this.noServers = numberOfServers;
        this.sharded = sharded;
        this.statsLogger = statsLogger;
        init();
    }


    /**
     * Initialize the net thread, mainly the server socket.
     *
     * @throws IOException
     */
    private void init() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(ip, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT); //server is in accepting mode to start with
    }


    /**
     * Run the net thread. This method won't terminate until the interrupt flag is set.
     */
    @Override
    public void run() {
        ByteBuffer buf = ByteBuffer.allocate(1048576); //allocate 2^20 byte buffer, should be large to handle most reasonable sized requests efficiently
        while (!Thread.currentThread().isInterrupted()) {
            try {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        if (!firstClientConnected && log) {
                            statsLogger.start();    //start stats logger as soon as the first connection is accepted
                            firstClientConnected = true;
                        }
                        logger.debug("accepting new connection");
                        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                        SocketChannel client = serverSocketChannel.accept();
                        client.configureBlocking(false);

                        client.register(selector, SelectionKey.OP_READ);    //register the selector for reading from the client
                        clients.put(client, new ClientInfo());
                    } else if (key.isReadable()) {
                        SocketChannel client = (SocketChannel) key.channel();  //get known client
                        buf.clear();

                        //read the input into a string
                        int l;
                        while ((l = client.read(buf)) > 0) {    //read until no more bytes to read
                            buf.clear();    //set position to 0
                            byte[] bufArr = new byte[l];
                            buf.get(bufArr);    //copy buffer into byte array
                            handle(bufArr, client);
                            buf.clear();    //set position to 0
                        }
                    } else {
                        logger.warn("got unknown key in net thread, ignoring it");
                    }
                }

            } catch (IOException e) {
                logger.warn("IOException in net thread (this is most likely due to shutdown or a client which closed the connection)", e);
            }
        }
        onShutdown();
    }


    /**
     * Handle incoming bytes
     *
     * @param bytes  the bytes read
     * @param client the client socket which sent the threads
     * @throws IOException
     */
    private void handle(byte[] bytes, SocketChannel client) throws IOException {
        if (writeToStream(bytes, client)) {
            ClientInfo clientInfo = clients.get(client);
            ByteArrayOutputStream byteStream = clientInfo.byteStream;
            MessageRequest msg = (MessageRequest) MessageParser.parse(byteStream.toByteArray(), client);
            if (msg != null) {
                logger.debug("parsing successful, adding reqeust to queue: " + msg);
                msg.setId(idCounter++); //set an id for the message (possible since netthread has global state)
                if (log) {
                    msg.setMsgInTs(clientInfo.arrivalTime);  //set timestamp when this message was received
                }
                if (msg instanceof MessageGetRequest) { //set the server id if necessary
                    if (!msg.isMultiGet() || (msg.isMultiGet() && !sharded)) {  //for a multiget the server id is only relevant if mode is not sharded, otherwise the request gets sent to all servers anyway
                        ((MessageGetRequest) msg).setServerId(getNextServerId());
                    }
                }
                RequestQueue.get().addRequest(msg);
                clientInfo.byteStream.reset();
            } else {   //incomplete request
                logger.debug("incomplete request, adding bytes to bytestream and continue listening");
            }
        }
    }

    /**
     * Holds some information about a client.
     * Contains a byte stream with the bytes read so far for this request.
     * The flag indicates, if this client is in a valid state at the moment. A state is considered invalid,
     * if garbage data has been sent and we're currently waiting for the new line to start parsing again.
     */
    private class ClientInfo {
        private final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        private boolean isInValidState = true;
        private long arrivalTime;   //the arrival time of the first message fragment
    }

    /**
     * Write the bytes to the byte stream of the client if they're part of a valid requst
     *
     * @param bytes  the bytes to check
     * @param client the client which sent the bytes
     * @return if the request was valid and should be parsed
     * @throws IOException
     */
    private boolean writeToStream(byte[] bytes, SocketChannel client) throws IOException {
        ClientInfo clientInfo = clients.get(client);
        ByteArrayOutputStream byteStream = clientInfo.byteStream;
        if (isValidStart(bytes) || (clientInfo.isInValidState && byteStream.size() > 0)) {//if this is a valid start or we're already read valid data, write bytes to the buffer
            byteStream.write(bytes);    //write the bytes into the byte stream
            clientInfo.isInValidState = true;
            if (log) {
                clientInfo.arrivalTime = System.nanoTime(); //set the timestamp once a valid start of a message is received
            }
            return true;
        } else {    //this is the start of garbage data
            clientInfo.isInValidState = false;
        }

        //at this point, valid state flag is false -> check if bytes contain a new line
        String str = new String(bytes, StandardCharsets.US_ASCII);
        if (!str.contains(NEW_LINE)) {   //no new line, i.e. still garbage data to discard
            logger.warn("unknown data has been received from client at " + client.getRemoteAddress().toString() + ", discarding it until new line character is received");
            return false;
        }

        int newLineFirstInd = str.indexOf(NEW_LINE);    //the index of the first new line
        if (newLineFirstInd == str.length() - NEW_LINE.length()) {    //only one new line at the end, i.e. discard this data but set valid flag, since after that possible valid data is incoming
            clientInfo.isInValidState = true;
            logger.warn("unknown data has been received from client at " + client.getRemoteAddress().toString() + ", discarding it until new line character is received");
            return false;
        }

        logger.warn("unknown data has been received from client at " + client.getRemoteAddress().toString() + ", discarding it until new line character is received");
        //there's a new line somewhere in between, i.e. recursively check the request starting from there for validity
        return writeToStream(Arrays.copyOfRange(bytes, newLineFirstInd + NEW_LINE.length(), bytes.length), client);

    }

    /**
     * Determine if the byte array starts with a valid command
     *
     * @param bytes the byte array to check
     * @return if valid start
     */
    private boolean isValidStart(byte[] bytes) {
        int length = bytes.length > 3 ? 3 : bytes.length;    //take at most the first 3 bytes to check
        String start = new String(bytes, 0, length, StandardCharsets.US_ASCII);    //parse the first 5 bytes to a string
        return start.startsWith(ProtocolConstants.GET_CMD) || start.startsWith(ProtocolConstants.SET_CMD);  //only valid if the command starts with set or get
    }

    /**
     * Get the next id of a server where a get request needs to be sent
     *
     * @return the server id
     */
    private int getNextServerId() {
        roundRobinCounter++;
        roundRobinCounter = roundRobinCounter % noServers;
        return roundRobinCounter;
    }

    /**
     * Called when the net thread is interrupted
     */
    private void onShutdown() {
        logger.debug("Net thread was interrupted, returning from run method. This is normal during shutdown!");
        closeSockets();
    }

    /**
     * Shutdown the net thread. Closes all sockets to the clients and the thread will stop listening to incoming requests.
     */
    private void closeSockets() {
        for (SocketChannel socketChannel : clients.keySet()) {
            try {
                socketChannel.close();
            } catch (IOException e) {
                logger.warn("failed to close client socket", e);
            }
        }
        try {
            selector.close();
        } catch (IOException e) {
            logger.warn("failed to close selector", e);
        }

        try {
            serverChannel.close();
        } catch (IOException e) {
            logger.warn("failed to close server socket", e);
        }
    }
}
