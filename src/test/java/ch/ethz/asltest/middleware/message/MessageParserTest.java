package ch.ethz.asltest.middleware.message;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests if the message parser is handling correct and incorrect/incomplete correctly.
 */
class MessageParserTest {
    /**
     * Test if correct get requests are parsed correctly.
     */
    @Test
    void testSingleGetRequestCorrect() {
        String keyGet = "foo";
        String keyGets = "bar";
        String get = "get " + keyGet + "\r\n";
        String gets = "gets " + keyGets + "\r\n";


        Message getMsg = MessageParser.parse(get.getBytes(), null);
        Message getsMsg = MessageParser.parse(gets.getBytes(), null);

        assertNotNull(getMsg);
        assertNotNull(getsMsg);

        assertTrue(getMsg instanceof MessageGetRequest);
        assertTrue(getsMsg instanceof MessageGetRequest);

        assertEquals(keyGet, ((MessageGetRequest) getMsg).getKeys().get(0));
        assertEquals(keyGets, ((MessageGetRequest) getsMsg).getKeys().get(0));
    }

    /**
     * Test if parsing incorrect/incomplete get requests returns null.
     */
    @Test
    void testSingleGetRequestIncorrect() {
        String cmd = "get bla"; //missing crlf
        assertNull(MessageParser.parse(cmd.getBytes(), null));
    }


    /**
     * Test if correct multi get requests are parsed correctly.
     */
    @Test
    void testMultiGetRequestCorrect() {
        List<String> keys = new ArrayList<>();
        keys.add("foo");
        keys.add("foo1");
        keys.add("foo2");
        keys.add("foo3");
        keys.add("foo4");
        String multiGet = buildGetRequest(keys, "get");
        String multiGets = buildGetRequest(keys, "gets");

        Message getMsg = MessageParser.parse(multiGet.getBytes(), null);
        Message getsMsg = MessageParser.parse(multiGets.getBytes(), null);

        assertNotNull(getMsg);
        assertNotNull(getsMsg);

        assertTrue(getMsg instanceof MessageGetRequest);
        assertTrue(getsMsg instanceof MessageGetRequest);

        for (int i = 0; i < keys.size(); i++) {
            assertEquals(keys.get(i), ((MessageGetRequest) getMsg).getKeys().get(i));
            assertEquals(keys.get(i), ((MessageGetRequest) getsMsg).getKeys().get(i));
        }
    }

    /**
     * Test if parsing incorrect/incomplete multi get requests returns null.
     */
    @Test
    void testMultiGetRequestIncorrect() {
        String cmd = "get bla foo bar2"; //missing crlf
        assertNull(MessageParser.parse(cmd.getBytes(), null));
    }

    /**
     * Build a get request
     *
     * @param keys the keys to test
     * @param cmd  the command (get or gets)
     * @return the command string
     */
    private String buildGetRequest(List<String> keys, String cmd) {
        StringJoiner sj = new StringJoiner(" ");
        sj.add(cmd);
        for (String key : keys) {
            sj.add(key);
        }
        String ret = sj.toString();
        ret += "\r\n";
        return ret;
    }

    /**
     * Test if correct get responses are parsed correctly.
     */
    @Test
    void testSingleGetResponseCorrect() {
        Map<String, String> kvpMap = new HashMap<>();
        String key1 = "foo1";
        String value1 = "blupblafoobar1";
        kvpMap.put(key1, value1);

        String req = buildGetResponse(kvpMap);

        Message reqMsg = MessageParser.parse(req.getBytes(), null);

        assertNotNull(reqMsg);

        assertTrue(reqMsg instanceof MessageGetResponse);

        MessageGetResponse messageGetResponse = (MessageGetResponse) reqMsg;
        assertFalse(messageGetResponse.isMultiGet());
        GetResponseValue getResponseValue = messageGetResponse.getGetResponseValues().get(0);
        assertEquals(5, getResponseValue.getFlags());
        assertEquals(key1, getResponseValue.getKey());
        String reqShould = "VALUE " + key1 + " 5 " + value1.length() + "\r\n" + value1 + "\r\n";
        assertEquals(reqShould, new String(getResponseValue.getBytes()));


        //test crlf within data block
        String cmd1 = "VALUE foo 0 5\r\n\r\nabc\r\nEND\r\n";
        assertNotNull(MessageParser.parse(cmd1.getBytes(), null));

        String cmd2 = "VALUE foo 0 5\r\n\r\na\r\n\r\nEND\r\n";
        assertNotNull(MessageParser.parse(cmd2.getBytes(), null));
    }

    /**
     * Test if parsing incorrect/incomplete get response returns null.
     */
    @Test
    void testSingleGetResponseIncorrect() {
        String cmd1 = "VALUE foo 0 5\r\nabcde\r\n"; //missing end block
        assertNull(MessageParser.parse(cmd1.getBytes(), null));

        String cmd2 = "VALUE foo 0 12\r\nabcde\r\nEND\r\n"; //byte count too high
        assertNull(MessageParser.parse(cmd2.getBytes(), null));

        String cmd3 = "VALUE foo 0 3\r\nabcde\r\nEND\r\n"; //byte count too low
        assertNull(MessageParser.parse(cmd3.getBytes(), null));

        String cmd4 = "VALUE foo 0 5\r\nabcdeEND\r\n"; //missing crlf after data block
        assertNull(MessageParser.parse(cmd4.getBytes(), null));
    }

    /**
     * Test if correct multi get responses are parsed correctly.
     */
    @Test
    void testMultiGetResponseCorrect() {
        Map<String, String> kvpMap = new HashMap<>();
        String key1 = "foo1";
        String value1 = "blupblsafdoobar1";
        String key2 = "foo2";
        String value2 = "blupblafasdagsddsfsoobar2";
        String key3 = "foo3";
        String value3 = "blupblasfoobar3";
        String key4 = "foo4";
        String value4 = "blasdupblafoobar4";
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.add(key3);
        keys.add(key4);
        values.add(value1);
        values.add(value2);
        values.add(value3);
        values.add(value4);
        kvpMap.put(key1, value1);
        kvpMap.put(key2, value2);
        kvpMap.put(key3, value3);
        kvpMap.put(key4, value4);

        String req = buildGetResponse(kvpMap);

        Message reqMsg = MessageParser.parse(req.getBytes(), null);

        assertNotNull(reqMsg);

        assertTrue(reqMsg instanceof MessageGetResponse);
        assertEquals(4, ((MessageGetResponse) reqMsg).getGetResponseValues().size());
        assertTrue(((MessageGetResponse) reqMsg).isMultiGet());

        for (int i = 0; i < 4; i++) {
            MessageGetResponse messageGetResponse = (MessageGetResponse) reqMsg;
            GetResponseValue getResponseValue = messageGetResponse.getGetResponseValues().get(i);
            assertEquals(5, getResponseValue.getFlags());
            assertEquals(keys.get(i), getResponseValue.getKey());
            String reqShould = "VALUE " + keys.get(i) + " 5 " + values.get(i).length() + "\r\n" + values.get(i) + "\r\n";
            assertEquals(reqShould, new String(getResponseValue.getBytes()));
        }
    }

    /**
     * Test if parsing incorrect/incomplete get response returns null.
     */
    @Test
    void testMultiGetResponseIncorrect() {
        String cmd1 = "VALUE foo 0 5\r\nabcde\r\nVALUE foo1 0 5\r\nabcde\r\n"; //missing end block
        assertNull(MessageParser.parse(cmd1.getBytes(), null));

        String cmd2 = "VALUE foo 0 5\r\nabcde\r\nVALUE foo1 0 5\r\nEND\r\n"; //missing data block in second value
        assertNull(MessageParser.parse(cmd2.getBytes(), null));
    }

    /**
     * Build a get request
     *
     * @param keyValueMap a map from keys to values
     * @return the command string
     */

    private String buildGetResponse(Map<String, String> keyValueMap) {
        StringJoiner sj = new StringJoiner("\r\n");

        for (Map.Entry<String, String> ent : keyValueMap.entrySet()) {
            sj.add("VALUE " + ent.getKey() + " 5 " + ent.getValue().length());
            sj.add(ent.getValue());
        }
        String ret = sj.toString();
        ret += "\r\nEND\r\n";
        return ret;
    }

    /**
     * Test if correct set requests are parsed correctly.
     */
    @Test
    void testSetRequestCorrect() {
        StringJoiner sj = new StringJoiner(" ");
        String key = "foobar";
        int flags = 4;
        long expTime = 100000;
        String value = "fodaskj\r\ndajsld jlkas jdwoo"; //with crlf in data block

        sj.add("set");
        sj.add(key);
        sj.add(flags + "");
        sj.add(expTime + "");
        sj.add(value.length() + "");

        String set = sj.toString() + "\r\n" + value + "\r\n";

        Message setMsg = MessageParser.parse(set.getBytes(), null);

        assertNotNull(setMsg);

        assertTrue(setMsg instanceof MessageSetRequest);
        MessageSetRequest setMsgReq = (MessageSetRequest) setMsg;

        assertEquals(key, setMsgReq.getKey());
        assertEquals(flags, setMsgReq.getFlags());
        assertEquals(expTime, setMsgReq.getExptime());
        assertEquals(set, new String(setMsgReq.getBytes()));
    }

    /**
     * Test if parsing incorrect/incomplete set response returns null.
     */
    @Test
    void testSetResponseIncorrect() {
        String cmd1 = "set foo 0 0 3\r\nabcde\r\n"; //byte count too low
        assertNull(MessageParser.parse(cmd1.getBytes(), null));

        String cmd2 = "set foo 0 0 7\r\nabcde\r\n"; //byte count too high
        assertNull(MessageParser.parse(cmd2.getBytes(), null));

        String cmd3 = "set foo 0 0 15\r\nabcde\r\n"; //byte count too low
        assertNull(MessageParser.parse(cmd3.getBytes(), null));

        String cmd4 = "set foo 0 0 15\r\n"; //missing data block
        assertNull(MessageParser.parse(cmd4.getBytes(), null));
    }

    /**
     * Test if correct set responses are parsed correctly.
     */
    @Test
    void testSetResponseCorrect() {
        String stored = "STORED\r\n";
        String notStored = "NOT_STORED\r\n";
        String exits = "EXISTS\r\n";
        String notFound = "NOT_FOUND\r\n";

        Message storedMsg = MessageParser.parse(stored.getBytes(), null);
        Message notStoredMsg = MessageParser.parse(notStored.getBytes(), null);
        Message existsMsg = MessageParser.parse(exits.getBytes(), null);
        Message notFoundMsg = MessageParser.parse(notFound.getBytes(), null);

        assertNotNull(storedMsg);
        assertNotNull(notStoredMsg);
        assertNotNull(existsMsg);
        assertNotNull(notFoundMsg);

        assertTrue(storedMsg instanceof MessageSetResponse);
        assertTrue(notStoredMsg instanceof MessageSetResponse);
        assertTrue(existsMsg instanceof MessageSetResponse);
        assertTrue(notFoundMsg instanceof MessageSetResponse);

        assertEquals(stored.trim(), ((MessageSetResponse) storedMsg).getMessage());
        assertEquals(notStored.trim(), ((MessageSetResponse) notStoredMsg).getMessage());
        assertEquals(exits.trim(), ((MessageSetResponse) existsMsg).getMessage());
        assertEquals(notFound.trim(), ((MessageSetResponse) notFoundMsg).getMessage());
    }

    /**
     * Test if correct errors are parsed correctly.
     */
    @Test
    void testErrorsCorrect() {
        String normalError = "ERROR\r\n";
        String clientError = "CLIENT_ERROR\r\n";
        String serverError = "SERVER_ERROR\r\n";

        Message normalErrorMsg = MessageParser.parse(normalError.getBytes(), null);
        Message clientErrorMsg = MessageParser.parse(clientError.getBytes(), null);
        Message serverErrorMsg = MessageParser.parse(serverError.getBytes(), null);

        assertNotNull(normalErrorMsg);
        assertNotNull(clientErrorMsg);
        assertNotNull(serverErrorMsg);

        assertTrue(normalErrorMsg instanceof MessageError);
        assertTrue(clientErrorMsg instanceof MessageError);
        assertTrue(serverErrorMsg instanceof MessageError);

        assertEquals(normalError.trim(), ((MessageError) normalErrorMsg).getErrorString());
        assertEquals(clientError.trim(), ((MessageError) clientErrorMsg).getErrorString());
        assertEquals(serverError.trim(), ((MessageError) serverErrorMsg).getErrorString());
    }

    /**
     * Test random stuff which should return null
     */
    @Test
    void testRandomStuff() {
        String cmd1 = "ERR"; //incomplete error
        assertNull(MessageParser.parse(cmd1.getBytes(), null));

        String cmd2 = "CLIENT_ERROR"; //error without crlf
        assertNull(MessageParser.parse(cmd2.getBytes(), null));

        String cmd3 = "NOT_STORED"; //not stored without crlf
        assertNull(MessageParser.parse(cmd3.getBytes(), null));

        String cmd4 = "getset"; //weird command
        assertNull(MessageParser.parse(cmd4.getBytes(), null));
    }

}