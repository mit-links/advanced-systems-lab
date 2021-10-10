package ch.ethz.asltest.middleware.internal;

import java.nio.charset.StandardCharsets;

/**
 * Contains some statically accessible constants.
 */
public class ProtocolConstants {
    public static final String SET_CMD = "set";
    public static final String GET_CMD = "get";
    public static final String GETS_CMD = "gets";
    public static final String ELEMENT_SEP = " ";
    public static final String NEW_LINE_SEP = "\r\n";
    public static final int NEW_LINE_SEP_NO_BYTES = NEW_LINE_SEP.getBytes(StandardCharsets.US_ASCII).length;

    public static final String END = "END";
    public static final String VALUE = "VALUE";
    public static final String STORED = "STORED";
    public static final String NOT_STORED = "NOT_STORED";
    public static final String EXISTS = "EXISTS";
    public static final String NOT_FOUND = "NOT_FOUND";

    public static final String ERROR = "ERROR";
    public static final String CLIENT_ERROR = "CLIENT_ERROR";
    public static final String SERVER_ERROR = "SERVER_ERROR";
}
