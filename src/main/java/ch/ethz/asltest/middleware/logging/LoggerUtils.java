package ch.ethz.asltest.middleware.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RandomAccessFileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.impl.MutableLogEvent;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Constants and static functions used for logging.
 */
class LoggerUtils {
    //the encoding of the three request types
    static final int GET_IND = 0;
    static final int MULTIGET_IND = 1;
    static final int SET_IND = 2;

    static final String ELEMENT_SEP = ",";  //the element separator
    static final int LOG_INTERVAL = 5;  //the logging interval in seconds
    static final int RESP_TIME_HISTOGRAM_BUCKET_SIZE = 100000;  //the response time histogram bucket size in ns (100 microseconds)
    static final int MIN_BUCKET_SIZE = 10;  //the minimum bucket size for the histogram
    static final String COMMENT_PREFIX = "#";   //all comments have to start with this string
    static final String PART_SEPARATOR = "=========================================";    //separates different parts in the log file
    static final String END_OF_BATCH = "END_OF_BATCH";

    //file/folder names
    static final String LOGGING_FOLDER = "logs";
    static final String RAW_LOGGING_FILE = "raw.log";
    static final String LOG4J_CONFIG_FILE_PATH = "src/main/resources/log4j/log4j2.xml";

    //indices in raw data for requests
    static final int INTERVAL_IND = 0;
    static final int ID_IND = 1;
    static final int RESPONSE_TIME_IND = 2;
    static final int QUEUE_WAIT_TIME_IND = 3;
    static final int SERVICE_TIME_IND = 4;
    static final int REQUEST_INDEX_IND = 5;
    static final int HITS_IND = 6;
    static final int MISSES_IND = 7;
    static final int NO_KEYS_IND = 8;

    //indices in raw data for queue length
    static final int QUEUE_LENGTH_IND = 1;

    /**
     * Flush a logger. This needs to be done by sending an empty packet with the end-of-batch flag set.
     *
     * @param loggerName the name of the logger to flush
     * @throws IOException
     */
    static void flush(String loggerName) throws IOException {

        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ConfigurationFactory cf = ConfigurationFactory.getInstance();
        InputStream is;
        is = new FileInputStream(LOG4J_CONFIG_FILE_PATH);

        Configuration config = cf.getConfiguration(ctx, new ConfigurationSource(is));
        config.start();
        RandomAccessFileAppender ap = config.getAppender(loggerName);

        StringBuilder sb = new StringBuilder();
        sb.append(COMMENT_PREFIX + END_OF_BATCH);
        LogEvent logEvent = new MutableLogEvent(sb, new Object[0]);
        logEvent.setEndOfBatch(true);   //this flag triggers flushing
        ap.append(logEvent);

    }
}
