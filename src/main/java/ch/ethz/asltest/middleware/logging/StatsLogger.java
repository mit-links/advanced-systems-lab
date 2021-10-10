package ch.ethz.asltest.middleware.logging;

import ch.ethz.asltest.middleware.message.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static ch.ethz.asltest.middleware.logging.LoggerUtils.*;

/**
 * Logs all necessary information to file. From this (potentially large) file, any statistics should be able to get aggregated.
 * <p>
 * One line in the file corresponds to one log event. A log event has one of the following forms (which can be distinguished by the number of elements)
 * <p>
 * 1 Message):
 * interval, id, responseTime, queueWaitTime, serviceTime, requestIndex, (no hits, no misses, avg number of keys in get)?
 * the last to elements are optional and only needed if the message is a (multi)get
 * the request index is defined as follow: 0=get, 1=multiget, 2=set
 * <p>
 * 2 Queue length):
 * interval, length
 * This value is queried by the thread at the end of every interval
 */
public class StatsLogger extends Thread {
    private static final Logger logger = LogManager.getLogger(StatsLogger.class);
    private static final String MSG_LOGGER_NAME = "msg_log";
    private static final Logger msgLogger = LogManager.getLogger(MSG_LOGGER_NAME);

    private int intervalCounter = 0;

    /**
     * Runs the log thread which only records the queue size in a predetermined interval.
     * This method only returns, if the thread is interrupted.
     */
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(LOG_INTERVAL * 1000);  //ms
                intervalCounter++;  //increment interval
            } catch (InterruptedException e) {
                onShutdown();
                return;
            }
        }
        onShutdown();
    }

    /**
     * Log all necessary information from a message
     *
     * @param request the request
     * @param resp    the response to the request
     */
    public void logMessage(MessageRequest request, MessageResponse resp) {
        if (resp == null || resp instanceof MessageSetResponse) {  //no need for cache hit/misses
            log(request.getId(), request.getMsgOutTs() - request.getMsgInTs(), request.getQueueOutTs() - request.getQueueInTs(),
                    request.getFromServerTs() - request.getToServerTs(), SET_IND);
        } else if (resp instanceof MessageGetResponse) {
            int reqInd = request.isMultiGet() ? MULTIGET_IND : GET_IND; //assign correct index dependning on it request is a multiget or not
            int hits = ((MessageGetResponse) resp).getGetResponseValues().size();
            int noKeys = ((MessageGetRequest) request).getKeys().size();    //number of requested keys
            int misses = noKeys - hits;
            log(request.getId(), request.getMsgOutTs() - request.getMsgInTs(), request.getQueueOutTs() - request.getQueueInTs(),
                    request.getFromServerTs() - request.getToServerTs(), reqInd, hits, misses, noKeys);
        } else {
            logger.warn("unexpected message to log: " + resp.getClass());
        }
    }

    /**
     * Log the queue size
     *
     * @param size the queue size
     */
    public void logQueueSize(int size) {
        msgLogger.info(intervalCounter + ELEMENT_SEP + size);
    }

    /**
     * Log a set request (which doesn't have cache hits and misses)
     *
     * @param id            the request id
     * @param respTime      the response time of the request
     * @param queueWaitTime the queue wait time of the request
     * @param serviceTime   the service time of the request
     * @param reqInd        the request index (get=0, multiget=1, set=2), see LoggerUtils
     */
    private void log(long id, long respTime, long queueWaitTime, long serviceTime, int reqInd) {
        String toLog = intervalCounter + ELEMENT_SEP + id + ELEMENT_SEP + respTime + ELEMENT_SEP + queueWaitTime + ELEMENT_SEP + serviceTime + ELEMENT_SEP + reqInd;
        msgLogger.info(toLog);
    }

    /**
     * Log a (multi)get request (which has cache hits/misses)
     *
     * @param id            the request id
     * @param respTime      the response time of the request
     * @param queueWaitTime the queue wait time of the request
     * @param serviceTime   the service time of the request
     * @param reqInd        the request index (get=0, multiget=1, set=2), see LoggerUtils
     * @param hits          the number of cache hits
     * @param misses        the number of cache misses
     * @param noKeys        the number of keys requested
     */
    private void log(long id, long respTime, long queueWaitTime, long serviceTime, int reqInd, int hits, int misses, int noKeys) {
        String toLog = intervalCounter + ELEMENT_SEP + id + ELEMENT_SEP + respTime + ELEMENT_SEP + queueWaitTime + ELEMENT_SEP + serviceTime + ELEMENT_SEP + reqInd + ELEMENT_SEP + hits + ELEMENT_SEP + misses + ELEMENT_SEP + noKeys;
        msgLogger.info(toLog);
    }

    /**
     * Called when the log thread is interrupted
     */
    private void onShutdown() {
        logger.debug("Stats logger thread was interrupted. This is normal during shutdown!");

        try {
            LoggerUtils.flush(MSG_LOGGER_NAME);
        } catch (IOException e) {
            logger.error("Error while adding end of batch message to raw log file");
        }
    }
}
