package ch.ethz.asltest.middleware.internal;

import ch.ethz.asltest.middleware.logging.StatsLogger;
import ch.ethz.asltest.middleware.message.*;
import ch.ethz.asltest.middleware.networking.IOHelpers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Contains a request queue and the worker thread implementation as a private class.
 * This class is a Singleton.
 */
public class RequestQueue {
    private static final Logger logger = LogManager.getLogger(RequestQueue.class);
    private static final int INITIAL_BUFFER_SIZE = 10000;

    private StatsLogger statsLogger;
    private BlockingQueue<MessageRequest> requestQueue;
    private List<WorkerThread> workerThreads;
    private boolean readSharded;
    private boolean log;
    private final Map<Integer, AtomicLong> allGetRequestsPerServer = new ConcurrentHashMap<>();

    private static RequestQueue instance;

    /**
     * Private constructor for the singleton
     */
    private RequestQueue() {
    }

    /**
     * Since this class is a singleton, we need a static way of getting the instance.
     *
     * @return the instance of this class.
     */
    public static RequestQueue get() {
        if (instance == null) {
            instance = new RequestQueue();
        }
        return instance;
    }


    /**
     * Initialize the request queue and starts the worker threads. Needs to be called before using the queue.
     *
     * @param numThreadsPTP the number of worker threads
     * @param mcAddresses   the server addresses
     * @param readSharded   if reads are sharded
     * @param log           if logging is enabled
     * @param statsLogger   the stats logger or null if logging is disabled
     */
    public void start(int numThreadsPTP, List<String> mcAddresses, boolean readSharded, boolean log, StatsLogger statsLogger) throws IOException {
        workerThreads = new ArrayList<>();
        requestQueue = new LinkedBlockingQueue<>();
        this.log = log;
        this.readSharded = readSharded;
        this.statsLogger = statsLogger;
        for (int i = 0; i < mcAddresses.size(); i++) {
            allGetRequestsPerServer.put(i, new AtomicLong());
        }
        for (int i = 0; i < numThreadsPTP; i++) {   //initialize and start all worker threads
            WorkerThread t = new WorkerThread(mcAddresses, log);
            workerThreads.add(t);
            t.start();
        }
    }

    /**
     * Add a request to the queue and invoke a worker thread to handle it
     *
     * @param request the request to add
     */
    void addRequest(MessageRequest request) {
        if (log) {
            request.setQueueInTs(System.nanoTime());
        }
        requestQueue.add(request);
        if (log) {
            statsLogger.logQueueSize(requestQueue.size());
        }
    }

    /**
     * Stop the worker threads and close their sockets.
     */
    public void shutdown() {
        for (WorkerThread t : workerThreads) {
            t.interrupt();  ////this causes the worker threads to close the sockets to the servers and return from the run method
            try {
                t.join();   //wait for thread to return
            } catch (InterruptedException e) {
                logger.error("Interruption occurred while shutting down worker threads.", e);
            }
        }
        if (log) {
            allGetRequestsPerServer.forEach((k, v) -> logger.info("server " + k + " had " + v + " single get and" +
                    " non-sharded multiget requests"));
        }
    }


    /**
     * The worker thread object that takes objects out of the request queue and handles them.
     */
    private class WorkerThread extends Thread {
        private final Logger logger = LogManager.getLogger(WorkerThread.class);

        private final List<SocketChannel> mcServers = new ArrayList<>();
        private ByteBuffer byteBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE); //each thread has its own buffer which it reuses
        private Map<Integer, Long> getRequestsPerServer = new HashMap<>();    //used for counting the number of get requests per server

        //logging stuff
        private final boolean log;

        WorkerThread(List<String> mcAddresses, boolean log) throws IOException {
            this.log = log;
            for (int i = 0; i < mcAddresses.size(); i++) {
                getRequestsPerServer.put(i, 0L);
            }
            init(mcAddresses);  //initialize the server addresses
        }

        /**
         * Parse the addresses of the servers and open sockets to them
         *
         * @param mcAddresses a list of server addresses of the form ip:port
         * @throws IOException
         */
        private void init(List<String> mcAddresses) throws IOException {
            for (String adr : mcAddresses) {
                String[] ipAndPort = adr.split(":");
                if (ipAndPort.length != 2) {
                    logger.fatal("receiver malformed ip and port: " + adr + ", shutting down");
                    System.exit(1);
                }
                int port = -1;
                try {
                    port = Integer.parseInt(ipAndPort[1]);
                } catch (NumberFormatException e) {
                    logger.fatal("receiver port: " + adr + ", shutting down", e);
                    System.exit(1);
                }
                InetSocketAddress mcAddress = new InetSocketAddress(ipAndPort[0], port);
                SocketChannel mcServer = null;
                try {
                    mcServer = SocketChannel.open(mcAddress);
                } catch (IOException e) {
                    logger.fatal("could not connect to mcServer at: " + mcAddress, e);
                    System.exit(1);
                }
                logger.debug("Worker thread opened connection to server " + mcServer.getRemoteAddress());
                mcServer.configureBlocking(true);
                mcServers.add(mcServer);
            }
        }

        /**
         * Run the thread. This method won't return until the worker thread is interrupted.
         */
        @Override
        public void run() {
            logger.debug("Worker thread  started");
            while (!Thread.currentThread().isInterrupted()) {
                logger.debug("Thread is waiting for incoming request");
                MessageRequest request;
                try {
                    request = requestQueue.take();  //this is blocking
                    if (log) {
                        request.setQueueOutTs(System.nanoTime());
                    }

                } catch (InterruptedException e) {
                    onShutdown();
                    return;
                }

                logger.debug("Thread is handling request: " + request);

                try {
                    handle(request);
                } catch (IOException e) {
                    logger.error("sending request to server failed: " + request, e);
                }
            }
            onShutdown();
        }

        /**
         * Handle a request, i.e. send request to server(s), collect response(s) and send response back to the client.
         *
         * @param request the request
         * @throws IOException
         */
        private void handle(MessageRequest request) throws IOException {
            MessageResponse resp;
            if (request instanceof MessageSetRequest) {
                resp = handleSet((MessageSetRequest) request);
            } else if (request instanceof MessageGetRequest) {
                MessageGetRequest getRequest = (MessageGetRequest) request;
                if (getRequest.isMultiGet()) {
                    resp = handleMultiGet(getRequest);
                } else {
                    resp = handleGet(getRequest, false);
                }
            } else {
                logger.warn("unknown message, ignoring it: " + request);
                return;
            }

            if (log) {  //log information
                request.setMsgOutTs(System.nanoTime());
                statsLogger.logMessage(request, resp);
            }
        }

        /**
         * Handle a set request.
         *
         * @param request the set request
         * @return the response object which was sent back to the client
         * @throws IOException
         */
        private MessageResponse handleSet(MessageSetRequest request) throws IOException {
            logger.debug("handling set...");
            SocketChannel client = request.getClient();

            logger.debug("sending set to all servers");
            writeToBuffer(request.getBytes());
            IOHelpers.sendRequestToServers(mcServers, byteBuffer);
            if (log) {
                request.setToServerTs(System.nanoTime());
            }

            logger.debug("collecting responses to set from all servers");
            ByteArrayOutputStream[] responsesByteStream = IOHelpers.getResponseFromServers(mcServers, byteBuffer);
            MessageSetResponse[] responses = new MessageSetResponse[mcServers.size()];

            for (int i = 0; i < responsesByteStream.length; i++) {
                ByteArrayOutputStream byteStream = responsesByteStream[i];
                Message resp = MessageParser.parse(byteStream.toByteArray(), client);
                while (resp == null) {
                    logger.debug("response not complete yet, retrying..");
                    ByteArrayOutputStream byteStreamNext = IOHelpers.getResponseFromServer(mcServers.get(i), byteBuffer);
                    byteStreamNext.writeTo(byteStream); //append to other byte stream
                    resp = MessageParser.parse(byteStream.toByteArray(), client);
                }
                if (!(resp instanceof MessageSetResponse)) {
                    if (resp instanceof MessageError) {
                        logger.debug("got error from server after set request");
                    } else {
                        logger.warn("receiver unexpected response from server: " + resp);
                    }
                    if (log) {
                        request.setFromServerTs(System.nanoTime());
                    }
                    writeToBuffer(resp.getBytes());
                    IOHelpers.sendRequestToServer(client, byteBuffer);
                    return (MessageResponse) resp;
                }
                responses[i] = (MessageSetResponse) resp;
            }
            if (log) {
                request.setFromServerTs(System.nanoTime());
            }

            MessageResponse response = null;
            logger.debug("sending response of set request to client");
            for (int i = 0; i < responses.length; i++) {
                if (i + 1 == responses.length || !responses[i].isSuccess()) {   //send msg to client if it's an error or the last received message
                    response = responses[i];
                    writeToBuffer(response.getBytes());
                    IOHelpers.sendRequestToServer(client, byteBuffer);

                    break;
                }
            }
            logger.debug("handling set done");
            return response;
        }

        /**
         * Handle a get request which can be a multiget.
         *
         * @param request  the get request
         * @param multiGet if the parameter is a multiget
         * @return the response object which was sent back to the client
         * @throws IOException
         */
        private MessageResponse handleGet(MessageGetRequest request, boolean multiGet) throws IOException {
            logger.debug("handling get...");

            int index = request.getServerId();
            if (log) {
                long old = getRequestsPerServer.get(index);
                getRequestsPerServer.put(index, old + 1);
            }
            SocketChannel server = mcServers.get(index);

            SocketChannel client = request.getClient();

            logger.debug("sending get to server: " + server.getRemoteAddress());
            writeToBuffer(request.getBytes());
            IOHelpers.sendRequestToServer(server, byteBuffer);
            if (log) {
                request.setToServerTs(System.nanoTime());
            }

            logger.debug("collecting responses to set from all servers");
            MessageResponse resp = getResponseFromServerForGet(server, client);
            if (log) {
                request.setFromServerTs(System.nanoTime());
            }

            logger.debug("sending response of get request to client");
            writeToBuffer(resp.getBytes());
            IOHelpers.sendRequestToServer(client, byteBuffer);

            logger.debug("handling get done");
            return resp;
        }

        /**
         * Handle a multi get request. If the mode is non-sharded, this behaves like handleGet.
         *
         * @param request the multi get request
         * @return the response object which was sent back to the client
         * @throws IOException
         */
        private MessageResponse handleMultiGet(MessageGetRequest request) throws IOException {
            if (readSharded) {
                logger.debug("handling sharded multi get by using get method");
                Map<String, GetResponseValue> responses = new HashMap<>();
                SocketChannel client = request.getClient();
                List<List<String>> listOfListofKeys = splitForShardedRead(request); //contains a list of keys for each server

                //send split requests to all servers
                logger.debug("sending split requests to servers");
                for (int i = 0; i < listOfListofKeys.size(); i++) {
                    List<String> listOfKeys = listOfListofKeys.get(i);
                    if (listOfKeys.size() == 0) {    //if no keys in this list (happens when less keys than servers), skip it
                        continue;
                    }
                    SocketChannel server = mcServers.get(i);
                    logger.debug("requesting " + listOfKeys.size() + " keys from server " + server.getRemoteAddress());

                    //build multi get request for the keys in the list
                    StringJoiner sj = new StringJoiner(ProtocolConstants.ELEMENT_SEP);
                    sj.add(ProtocolConstants.GET_CMD);
                    for (String key : listOfKeys) {
                        sj.add(key);
                    }
                    String reqStr = sj.toString() + ProtocolConstants.NEW_LINE_SEP;

                    writeToBuffer(reqStr.getBytes(StandardCharsets.US_ASCII));
                    IOHelpers.sendRequestToServer(server, byteBuffer);
                }
                if (log) {
                    request.setToServerTs(System.nanoTime());
                }

                //collect responses from servers
                MessageError error = null;
                logger.debug("collecting responses from server");
                for (int i = 0; i < listOfListofKeys.size(); i++) {
                    List<String> listOfKeys = listOfListofKeys.get(i);
                    if (listOfKeys.size() == 0) {    //if no keys in this list (happens when less keys than servers), skip it
                        continue;
                    }
                    SocketChannel server = mcServers.get(i);
                    logger.debug("collecting response from server " + server.getRemoteAddress());

                    MessageResponse resp = getResponseFromServerForGet(server, client);
                    if (resp instanceof MessageError) { //in case of an error, save the message for later. We can't return here, because we have to receive the answer of all the servers first
                        error = (MessageError) resp;
                    } else {
                        for (GetResponseValue getResponseValue : ((MessageGetResponse) resp).getGetResponseValues()) {  //put in the map so we can later sort the responses
                            responses.put(getResponseValue.getKey(), getResponseValue);
                        }
                    }
                }
                if (log) {
                    request.setFromServerTs(System.nanoTime());
                }
                if (error != null) {    //send the received error to the client
                    writeToBuffer(error.getBytes());
                    IOHelpers.sendRequestToServer(client, byteBuffer);
                    return error;
                }

                //put together the response and sort the keys to be in the correct order
                logger.debug("all responses from servers have been collected, now putting them together");
                List<GetResponseValue> responseValues = new ArrayList<>();  //needed so the input order is preserved
                for (String key : request.getKeys()) {
                    if (responses.containsKey(key)) {   //not all requested keys may have been in the db, i.e. check first if they were actually found
                        responseValues.add(responses.get(key));
                    }
                }

                MessageGetResponse messageGetResponse = new MessageGetResponse(responseValues);

                writeToBuffer(messageGetResponse.getBytes());
                IOHelpers.sendRequestToServer(client, byteBuffer);

                logger.debug("handling sharded multi get done");
                return messageGetResponse;

            } else {
                logger.debug("handling non-sharded multi get by using get method");
                return handleGet(request, true); //handle as a regular get request, just with multiple keys
            }
        }

        /**
         * Get the response from a server after get request
         *
         * @param server the server socket
         * @param client the client socket
         * @return the response Message which might be an error message.
         * @throws IOException
         */
        private MessageResponse getResponseFromServerForGet(SocketChannel server, SocketChannel client) throws IOException {
            ByteArrayOutputStream responsesByteStream = IOHelpers.getResponseFromServer(server, byteBuffer);

            Message resp = MessageParser.parse(responsesByteStream.toByteArray(), client);
            while (resp == null) {
                logger.debug("response not complete yet, retrying..");
                ByteArrayOutputStream byteStreamNext = IOHelpers.getResponseFromServer(server, byteBuffer);
                byteStreamNext.writeTo(responsesByteStream); //append to other byte stream
                resp = MessageParser.parse(responsesByteStream.toByteArray(), client);
            }
            if (!(resp instanceof MessageGetResponse)) {
                if (resp instanceof MessageError) {
                    logger.warn("got error from server after get request");
                } else {
                    logger.error("receiver unexpected response from server: " + resp);
                }
                writeToBuffer(resp.getBytes());
                IOHelpers.sendRequestToServer(client, byteBuffer);
                return (MessageResponse) resp;
            }
            return (MessageGetResponse) resp;
        }


        /**
         * Splits the keys in a multi get into mcServers.size() different balanced subsets.
         *
         * @param multiGetRequest the multi get request
         * @return a list of lists of keys, one list for each mc server
         */
        private List<List<String>> splitForShardedRead(MessageGetRequest multiGetRequest) {
            List<String> keys = multiGetRequest.getKeys();
            List<List<String>> splitKeys = new ArrayList<>(mcServers.size());
            for (int i = 0; i < mcServers.size(); i++) {  //one key list per server
                splitKeys.add(new ArrayList<>());
            }
            int j = 0;
            for (int i = 0; i < multiGetRequest.getKeys().size(); i++) {   //add keys to list for servers
                splitKeys.get(j).add(keys.get(i));
                j++;
                j %= mcServers.size();
            }
            return splitKeys;
        }

        /**
         * Write the byte array to the buffer. If the buffer isn't large enough, allocate a new one twice the size of the bytes array
         *
         * @param bytes the bytes array
         */
        private void writeToBuffer(byte[] bytes) {
            if (bytes.length > byteBuffer.capacity()) {
                byteBuffer = ByteBuffer.allocate(2 * bytes.length);
            }
            byteBuffer.clear();
            byteBuffer.put(bytes);
            byteBuffer.flip();
        }

        /**
         * Called when the worker thread is interrupted
         */
        private void onShutdown() {
            logger.debug("Worker thread " + Thread.currentThread().getId() + " was interrupted, returning from run method. " +
                    "This is normal during shutdown!");
            if (log) {
                for (Map.Entry<Integer, Long> entry : getRequestsPerServer.entrySet()) {  //put local get request count in global map
                    int ind = entry.getKey();
                    allGetRequestsPerServer.get(ind).addAndGet(entry.getValue());
                }
            }
            closeSockets();
        }

        /**
         * Close the sockets to the servers
         */
        private void closeSockets() {
            for (SocketChannel socketChannel : mcServers) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    logger.error("Error occurred while closing socket to server", e);
                }
            }
        }
    }
}
