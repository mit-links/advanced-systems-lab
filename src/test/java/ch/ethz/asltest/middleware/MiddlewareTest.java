package ch.ethz.asltest.middleware;

import ch.ethz.asltest.middleware.message.*;
import ch.ethz.asltest.middleware.networking.IOHelpers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the middleware as a whole. Simulates the memtier servers but requires 3 memcached servers to run.
 * The addresses of the servers need to be:
 * localhost:11221
 * localhost:11222
 * localhost:11223
 */
class MiddlewareTest {
    private static SocketChannel mwShardedSocket1;
    private static SocketChannel mwShardedSocket2;
    private static Middleware mwSharded1;

    private static List<String> mcServers;
    private static String ip;
    private static int portSharded;
    private static int noThreads;

    private static int valueLength = 64;
    private static boolean randomizeValues = true;

    private final static Random random = new Random();    //to create random keys and values
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    private final static String keyPrefix = "key";

    /**
     * Setup and connect to the middlewares.
     */
    @BeforeAll
    static void setUp() {
        mcServers = new ArrayList<>();
        mcServers.add("localhost:11221");
        mcServers.add("localhost:11222");
        mcServers.add("localhost:11223");
        ip = "localhost";
        portSharded = 12348;
        noThreads = 128; //number of worker threads (high to provoke race conditions)
        mwSharded1 = new Middleware(ip, portSharded, mcServers, noThreads, true, false); //use sharded because more interesting and don't do logging
        mwSharded1.run();
        try {
            mwShardedSocket1 = SocketChannel.open(new InetSocketAddress(ip, portSharded));
            mwShardedSocket1.configureBlocking(false);
            mwShardedSocket2 = SocketChannel.open(new InetSocketAddress(ip, portSharded));
            mwShardedSocket2.configureBlocking(false);
        } catch (IOException e) {
            e.printStackTrace();
            //System.exit(1);
        }
        try {
            Thread.sleep(1000); //wait until middleware is up and running
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reset the values which may have been altered
     */
    @BeforeEach
    void reset() {
        valueLength = 64;
        randomizeValues = true;
    }


    /**
     * Test a simple set with a get following and compare the to values.
     */
    @Test
    void testSimpleSetGet() throws IOException {
        //store some value
        String key = getNextKey();
        byte[] value = getNextValue();
        byte[] set = getSet(key, value);

        Message msgSet = getResponse(set);
        assertEquals(msgSet.getClass(), MessageSetResponse.class);

        MessageSetResponse respMsgSet = (MessageSetResponse) msgSet;
        assertEquals("STORED", respMsgSet.getMessage());

        //check if server returns the stored value
        byte[] get = getGet(key);
        Message msgGet = getResponse(get);

        assertEquals(MessageGetResponse.class, msgGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgGet;
        List<GetResponseValue> getResponseValues = respMsgGet.getGetResponseValues();
        assertFalse(respMsgGet.isMultiGet());
        GetResponseValue getValue = getResponseValues.get(0);
        assertEquals(key, getValue.getKey());

        //compare value byte per byte
        assertTrue(compareValueResponseRequest(value, getValue));
    }

    /**
     * Test an unknown key with single get
     */
    @Test
    void testUnknownSingleGet() throws IOException {
        String key = getNextKey();
        byte[] get = getGet(key);

        Message msgGet = getResponse(get);

        assertEquals(MessageGetResponse.class, msgGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgGet;
        assertEquals(0, respMsgGet.getGetResponseValues().size());
    }

    /**
     * Test an unknown key with multi get
     */
    @Test
    void testUnknownMultiGet() throws IOException {
        List<String> keys = new ArrayList<>();
        keys.add(getNextKey()); //Add some keys
        keys.add(getNextKey());
        keys.add(getNextKey());
        keys.add(getNextKey());
        keys.add(getNextKey());
        keys.add(getNextKey());
        byte[] get = getGet(keys);

        Message msgGet = getResponse(get);

        assertEquals(MessageGetResponse.class, msgGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgGet;
        assertEquals(0, respMsgGet.getGetResponseValues().size());  //response should be empty since none of the keys were stored
    }

    /**
     * Test two interleaved valid requests
     */
    @Test
    void testInterleavedValid() throws IOException, InterruptedException {
        String key1 = getNextKey();
        String key2 = getNextKey();
        byte[] value1 = getNextValue();
        valueLength = 100;  //different size than set 1
        byte[] value2 = getNextValue();

        byte[] set1 = getSet(key1, value1);
        byte[] set2 = getSet(key2, value2);

        int l = 60; //split after the header

        assert l <= set1.length && l<= set2.length;

        sendToServer(Arrays.copyOfRange(set1, 0, l), mwShardedSocket1);
        sendToServer(Arrays.copyOfRange(set2, 0, l), mwShardedSocket2);

        Thread.sleep(500);

        Message resp1 = getResponse(Arrays.copyOfRange(set1, l, set1.length), mwShardedSocket1);  //send rest
        Message resp2 = getResponse(Arrays.copyOfRange(set2, l, set2.length), mwShardedSocket2);  //send rest

        MessageSetResponse respMsgSet1 = (MessageSetResponse) resp1;
        assertEquals("STORED", respMsgSet1.getMessage());

        MessageSetResponse respMsgSet2 = (MessageSetResponse) resp2;
        assertEquals("STORED", respMsgSet2.getMessage());

    }

    /**
     * Test two interleaved requests, one is valid, the other sends garbage
     */
    @Test
    void testInterleavedWithGarbage() throws IOException, InterruptedException {
        String key1 = getNextKey();
        valueLength = 100;
        byte[] value1 = getNextValue();

        byte[] set1 = getSet(key1, value1);
        valueLength = 1000;
        byte[] garbage = getNextValue();

        int l = 60; //split after the header

        assert l <= set1.length;

        sendToServer(Arrays.copyOfRange(set1, 0, l), mwShardedSocket1);

        Thread.sleep(500);

        sendToServer(garbage, mwShardedSocket2);    //send garbage in between

        Message resp1 = getResponse(Arrays.copyOfRange(set1, l, set1.length), mwShardedSocket1);  //send rest

        MessageSetResponse respMsgSet1 = (MessageSetResponse) resp1;
        assertEquals("STORED", respMsgSet1.getMessage());
    }

    /**
     * Test simple get and sets with medium sized values.
     */
    @Test
    void testSimpleSetGetMedium() throws IOException {
        valueLength = 1000;    //1kB
        testSimpleSetGet();
    }

    /**
     * Test simple get and sets with large values.
     */
    @Test
    void testSimpleSetGetLarge() throws IOException {
        valueLength = 1000000;    //1MB (10MB is too large for memcached)
        testSimpleSetGet();
    }

    /**
     * Test a multi get with one stored value and one unknown one.
     */
    @Test
    void testMultiGetWithUnknowns() throws IOException {
        String key = getNextKey();
        byte[] value = getNextValue();
        byte[] set = getSet(key, value);

        Message msgSet = getResponse(set);
        assertEquals(msgSet.getClass(), MessageSetResponse.class);

        MessageSetResponse respMsgSet = (MessageSetResponse) msgSet;
        assertEquals("STORED", respMsgSet.getMessage());

        String unknownKey = getNextKey();   //a key that isn't stored in the db
        String unknownKey2 = getNextKey();   //a key that isn't stored in the db
        List<String> keys = new ArrayList<>();
        keys.add(unknownKey);
        keys.add(key);  //add actually stored key in the middle
        keys.add(unknownKey2);
        byte[] multiget = getGet(keys);

        Message msgGet = getResponse(multiget);

        assertEquals(MessageGetResponse.class, msgGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgGet;
        assertEquals(1, respMsgGet.getGetResponseValues().size());
        assertEquals(key, respMsgGet.getGetResponseValues().get(0).getKey());
        assertTrue(compareValueResponseRequest(value, respMsgGet.getGetResponseValues().get(0)));
    }

    /**
     * Test that the order of the keys in a sharded request gets preserved.
     *
     * @throws IOException
     */
    @Test
    void testShardedOrder() throws IOException {
        int size = 1000;
        List<String> keys = getNextKeys(size);
        byte[][] values = getNextValues(size);

        //send a set for all values
        for (int i = 0; i < size; i++) {
            byte[] set = getSet(keys.get(i), values[i]);
            Message msgSet = getResponse(set);
            assertEquals(MessageSetResponse.class, msgSet.getClass());

            MessageSetResponse respMsgSet = (MessageSetResponse) msgSet;
            assertEquals("STORED", respMsgSet.getMessage());
        }

        //send a multiget for all keys
        byte[] multiGet = getGet(keys);
        Message msgMultiGet = getResponse(multiGet);
        assertEquals(MessageGetResponse.class, msgMultiGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgMultiGet;
        assertEquals(values.length, respMsgGet.getGetResponseValues().size());

        for (int i = 0; i < values.length; i++) {
            assertTrue(compareValueResponseRequest(values[i], respMsgGet.getGetResponseValues().get(i)));
        }
    }


    /**
     * Test order of multi gets with medium sized values.
     */
    @Test
    void testShardedOrderMedium() throws IOException {
        valueLength = 500;    //1000*500B
        testShardedOrder();
    }

    /**
     * Test order of multi gets with large values.
     */
    @Test
    void testShardedOrderLarge() throws IOException {
        valueLength = 750;    //1000*750B, for 1kB memcached doesn't send all values back, probably deletes some
        testShardedOrder();
    }

    /**
     * Test that random data is correctly discarded. This tests, if a block of data with garbage at the start and a valid request following is handled correctly
     */
    @Test
    void testGarbageDataWithGet() throws IOException {
        String key = getNextKey();
        byte[] value = getNextValue();
        byte[] set = getSet(key, value);

        Message msgSet = getResponse(set);
        assertEquals(msgSet.getClass(), MessageSetResponse.class);

        MessageSetResponse respMsgSet = (MessageSetResponse) msgSet;
        assertEquals("STORED", respMsgSet.getMessage());

        byte[] randomGarbage = getNextValue();

        sendToServer(randomGarbage, mwShardedSocket1);  //send the garbage data to the server which should wait for the new line to discard it

        byte[] newLine = "\r\n".getBytes();

        sendToServer(newLine, mwShardedSocket1);  //new line

        //now a new request should be able to be received

        //check if server returns the stored value
        byte[] get = getGet(key);
        Message msgGet = getResponse(get);

        assertEquals(MessageGetResponse.class, msgGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgGet;
        List<GetResponseValue> getResponseValues = respMsgGet.getGetResponseValues();
        assertFalse(respMsgGet.isMultiGet());
        GetResponseValue getValue = getResponseValues.get(0);
        assertEquals(key, getValue.getKey());

        //compare value byte per byte
        assertTrue(compareValueResponseRequest(value, getValue));
    }

    /**
     * Test that random data is correctly discarded. Garbage, request garbage.
     */
    @Test
    void testGarbageDataMultipleNewLines() throws IOException {
        String key = getNextKey();
        byte[] value = getNextValue();
        byte[] set = getSet(key, value);


        Message msgSet = getResponse(set);
        assertEquals(msgSet.getClass(), MessageSetResponse.class);

        MessageSetResponse respMsgSet = (MessageSetResponse) msgSet;
        assertEquals("STORED", respMsgSet.getMessage());


        byte[] newLine = "\r\n".getBytes();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        //write some garbage data with new lines
        byteStream.write(getNextValue());
        byteStream.write(newLine);
        byteStream.write(getNextValue());
        byteStream.write(newLine);
        valueLength = 100000;   //add a very large garbage part
        byteStream.write(getNextValue());
        byteStream.write(newLine);
        byteStream.write(getGet(key));
        byte[] req = byteStream.toByteArray();

        Message msgGet = getResponse(req);  //should be the response to get last get because all the preceeding garbage should have been discarded

        assertEquals(MessageGetResponse.class, msgGet.getClass());

        MessageGetResponse respMsgGet = (MessageGetResponse) msgGet;
        List<GetResponseValue> getResponseValues = respMsgGet.getGetResponseValues();
        assertFalse(respMsgGet.isMultiGet());
        GetResponseValue getValue = getResponseValues.get(0);
        assertEquals(key, getValue.getKey());

        //compare value byte per byte
        assertTrue(compareValueResponseRequest(value, getValue));
    }


    /**
     * Send a request to the middleware in sharded mode and get the response
     *
     * @param req the request
     * @return the response
     * @throws IOException
     */
    private Message getResponse(byte[] req) throws IOException {
        return getResponse(req, mwShardedSocket1);
    }

    /**
     * Send a request to the middleware in sharded mode and get the response
     *
     * @param req    the request
     * @param socket the socket to send to
     * @return the response
     * @throws IOException
     */
    private Message getResponse(byte[] req, SocketChannel socket) throws IOException {
        sendToServer(req, socket);
        ByteArrayOutputStream responsesByteStream = IOHelpers.getResponseFromServer(socket, byteBuffer);

        Message resp = MessageParser.parse(responsesByteStream.toByteArray(), null);
        while (resp == null) {
            ByteArrayOutputStream byteStreamNext = IOHelpers.getResponseFromServer(socket, byteBuffer);
            byteStreamNext.writeTo(responsesByteStream); //append to other byte stream
            resp = MessageParser.parse(responsesByteStream.toByteArray(), null);
        }
        return resp;
    }

    /**
     * Builds a set request with the provided key and value
     *
     * @param key   the key as a string
     * @param value the value as a byte array
     * @return the request as a byte array
     */
    private byte[] getSet(String key, byte[] value) {
        String cmd = "set " + key + " 0 10000 " + value.length + "\r\n";
        byte[] ret = new byte[cmd.length() + value.length + 2];
        int i = 0;
        for (byte b : cmd.getBytes()) {
            ret[i++] = b;
        }
        for (byte b : value) {
            ret[i++] = b;
        }
        for (byte b : "\r\n".getBytes()) {
            ret[i++] = b;
        }
        return ret;
    }

    /**
     * Build a get request with the provided keys
     *
     * @param keys a list of keys
     * @return the byte array encoding the request
     */
    private byte[] getGet(List<String> keys) {
        StringJoiner sj = new StringJoiner(" ");
        for (String key : keys) {
            sj.add(key);
        }
        return ("get " + sj.toString() + "\r\n").getBytes();
    }

    /**
     * Compare the value of the request and the response and check if they're equal.
     *
     * @param should the value it should be as a byte array
     * @param resp   the response as a GetResponseValue object
     * @return if the values are equal
     */
    private boolean compareValueResponseRequest(byte[] should, GetResponseValue resp) {
        String str = new String(resp.getBytes(), StandardCharsets.US_ASCII);
        assertEquals(str.length(), resp.getBytes().length);
        int start = str.indexOf("\r\n") + 2;
        int stop = str.length() - 2;    //remove the trailing crlf
        byte[] respBytes = resp.getBytes();
        if (stop - start != should.length) {
            return false;
        }
        for (int i = 0; i < should.length; i++) {
            if (should[i] != respBytes[start + i]) {
                return false;
            }
        }
        return true;
    }


    /**
     * Build a get request with the provided key
     *
     * @param key a keys
     * @return the byte array encoding the request
     */
    private byte[] getGet(String key) {
        return ("get " + key + "\r\n").getBytes();
    }

    /**
     * Generates random key with the prefix "key"
     *
     * @return the generated key
     */
    private String getNextKey() {
        return keyPrefix + random.nextInt();
    }

    /**
     * Get multiple random keys
     *
     * @param size the number of keys
     * @return a list of keys
     */
    private List<String> getNextKeys(int size) {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ret.add(getNextKey());
        }
        return ret;
    }

    /**
     * Returns a random byte array of the specified length
     *
     * @return the generated random byte array
     */
    private byte[] getNextValue() {
        if (randomizeValues) {
            byte[] ret = new byte[valueLength];
            random.nextBytes(ret);
            return ret;
        } else {//use random ascii for debugging puproses

            char[] chars = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
            StringBuilder sb = new StringBuilder();
            Random random = new Random();
            for (int i = 0; i < valueLength; i++) {
                char c = chars[random.nextInt(chars.length)];
                sb.append(c);
            }
            String output = sb.toString();
            return output.getBytes(StandardCharsets.US_ASCII);
        }
    }

    /**
     * Returns a 2D byte array containing multiple randomly generated values
     *
     * @param size the number of values
     * @return the 2D byte array
     */
    private byte[][] getNextValues(int size) {
        byte[][] ret = new byte[size][valueLength];
        for (int i = 0; i < size; i++) {
            byte[] value = getNextValue();
            System.arraycopy(value, 0, ret[i], 0, valueLength);
        }
        return ret;
    }

    /**
     * Send a request to the server
     *
     * @param req    the byte array of the request
     * @param server the socket of the server
     * @throws IOException
     */
    private void sendToServer(byte[] req, SocketChannel server) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(req.length);
        byteBuffer.put(req);
        byteBuffer.clear();
        server.write(byteBuffer);
    }
}