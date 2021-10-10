package ch.ethz.asltest.middleware.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import static ch.ethz.asltest.middleware.logging.LoggerUtils.*;

/**
 * Upon shutdown, this class aggregates the raw logging data logged to a file by StatsLogger and writes the aggregated
 * data to file and prints out some interesting overall stats. All log files are written to the "logs" folder.
 * <p>
 * One line of the output file has the following format (no = number of):
 * intervalId, avg responseTime, avg queueWaitTime, avg queue size, avg serviceTime, noGets, noMultigets, noSets, no hits, no misses, avg no keys in get
 * <p>
 * After these lines there's a single sparator line:
 * ===========================
 * Following are the lines containing the data for the response time histogram. Each line has the following format
 * bucketId, number of requests in this bucket
 * The buckets start in the range [0,100 microseconds) and continue until there all response times are bucketized
 * Buckets with less than 5 responses are omitted
 */
public class StatsAggregator {
    private static final Logger logger = LogManager.getLogger(StatsAggregator.class);
    private static final String AGG_LOGGER_NAME = "agg_log";
    private static final Logger aggLogger = LogManager.getLogger(AGG_LOGGER_NAME);

    private final File in;

    private final Map<Integer, Integer> responseTimeBuckets = new TreeMap<>();
    private final Map<Integer, Stats> statsPerInterval = new TreeMap<>();   //maps interval ids to stats object, ordered by id

    private long lineCounter = 0L;

    /**
     * Create the reader for raw.log and create aggregated.log and the writer to it
     *
     * @
     */
    public StatsAggregator() {
        in = new File(LOGGING_FOLDER + File.separator + RAW_LOGGING_FILE);
    }

    /**
     * Aggregate the raw.log file. This means aggregating the raw data into smaller chunks (e.g. average the events
     * every5 seconds) which leads to a much smaller log file.
     */
    public void aggregate() throws IOException {
        long start = System.nanoTime();
        Date now = new Date(System.currentTimeMillis());
        logger.info("aggregating raw data");
        writeComment("LOGGED DATA AGGREGATED IN INTERVALS OF " + LOG_INTERVAL + " SECONDS AT " + now);
        writeComment("format (no=number of): intervalId, avg responseTime, avg queueWaitTime, avg queue size, avg serviceTime, noGets, noMultigets, noSets, no hits, no misses, avg no keys in get");
        Stream<String> inStream = Files.lines(in.toPath(), StandardCharsets.US_ASCII);
        inStream.forEach(this::handleLine);

        long startDump = System.nanoTime();

        //dump all intervals in order
        for (Map.Entry<Integer, Stats> entry : statsPerInterval.entrySet()) {
            entry.getValue().dump();
        }

        writeComment(PART_SEPARATOR);
        writeComment("HISTOGRAM OF RESPONSE TIMES IN BUCKETS OF " + RESP_TIME_HISTOGRAM_BUCKET_SIZE / 1000 + " MICROSECONDS (trailing buckets with less than " + MIN_BUCKET_SIZE + " elements omitted)");
        writeComment("format: bucketId, number of requests in this bucket");

        dumpHistogramData();

        LoggerUtils.flush(AGG_LOGGER_NAME);

        logger.info("aggregating raw data done, deleting raw log file");
        in.delete();    //delete raw file

        long end = System.nanoTime();
        double diffStart = (end - start) / 1000000000.0;    //ns to s
        double diffDump = (end - startDump) / 1000000000.0;    //ns to s
        logger.info("aggregating " + lineCounter + " lines took " + diffStart + " seconds in total, " + diffDump + " seconds for dumping");
    }

    /**
     * Handle a line, i.e. aggregate the data correctly and write it to the writer if an interval has passed
     *
     * @param line the line to handle
     */
    private void handleLine(String line) {
        lineCounter++;
        if (line.equals(END_OF_BATCH)) { //skip end of batch line
            return;
        }
        try {
            String[] els = line.split(ELEMENT_SEP);
            int interval = Integer.parseInt(els[INTERVAL_IND]);    //first element is interval

            if (!statsPerInterval.containsKey(interval)) {
                statsPerInterval.put(interval, new Stats(interval));
            }
            Stats curStats = statsPerInterval.get(interval);    //get the stats object for this interval

            if (els.length == 2) {  //queue size event
                curStats.queueSize += Integer.parseInt(els[QUEUE_LENGTH_IND]);
                curStats.queueSizeCounter++;

            } else if (els.length == 6 || els.length == 9) {   //msg  (with optional cache hit/miss numbers/number of requested keys)
                long responseTime = Long.parseLong(els[RESPONSE_TIME_IND]);
                addToHistogramMap(responseTime);
                curStats.responseTimeTotal += responseTime;
                curStats.responseTimeCounter++;
                curStats.queueWaitTimeTotal += Long.parseLong(els[QUEUE_WAIT_TIME_IND]);
                curStats.queueWaitTimeCounter++;
                curStats.serviceTimeTotal += Long.parseLong(els[SERVICE_TIME_IND]);
                curStats.serviceTimeCounter++;
                switch (Integer.parseInt(els[REQUEST_INDEX_IND])) {
                    case GET_IND:
                        curStats.getCounter++;
                        break;
                    case MULTIGET_IND:
                        curStats.multiCounter++;
                        break;
                    case SET_IND:
                        curStats.setCounter++;
                        break;
                    default:
                        logger.error("unexpected request index " + els[REQUEST_INDEX_IND] + " in line: " + line);
                        return;
                }

                if (els.length == 9) {   //there are cache miss/hit numbers
                    curStats.hitCounter += Integer.parseInt(els[HITS_IND]);
                    curStats.missCounter += Integer.parseInt(els[MISSES_IND]);
                    curStats.noKeysCounter += Integer.parseInt(els[NO_KEYS_IND]);
                }
            } else {
                logger.debug("unexpected length of line in raw log file, ignoring it: " + line);    //the last line might not have been logged completely
            }
        } catch (NumberFormatException e) {
            logger.error("error while parsing line of raw log file: " + line, e);
        }
    }


    /**
     * Add a response time to the correct bucket in the map
     *
     * @param responseTime the response time to add
     */
    private void addToHistogramMap(long responseTime) {
        int index = getHistogramBucketIndex(responseTime);
        int old = 0;
        if (responseTimeBuckets.containsKey(index)) {
            old = responseTimeBuckets.get(index);
        }
        responseTimeBuckets.put(index, old + 1);
    }

    /**
     * Get the bucket index of the response time
     *
     * @param responseTime the response time
     * @return the bucket index (start of range is inclusive, end exclusive)
     */
    private int getHistogramBucketIndex(long responseTime) {
        int i = 0;
        long c = responseTime;
        while (c >= RESP_TIME_HISTOGRAM_BUCKET_SIZE) {
            i++;
            c -= RESP_TIME_HISTOGRAM_BUCKET_SIZE;
        }
        return i;
    }

    /**
     * Write the data for the histogram to the writer
     */
    private void dumpHistogramData() {
        //delete all entries below the minimum bucket size
        List<Integer> toDelete = new ArrayList<>();  //collect the keys to delete
        for (Map.Entry<Integer, Integer> entry : responseTimeBuckets.entrySet()) {
            if (entry.getValue() < MIN_BUCKET_SIZE) {
                toDelete.add(entry.getKey());
            }
        }
        for (int i : toDelete) { //delete the collected values here, otherwise concurrent modification exception
            responseTimeBuckets.remove(i);
        }

        int last = -1;
        for (Map.Entry<Integer, Integer> entry : responseTimeBuckets.entrySet()) {  //map is sorted
            while (entry.getKey() != last + 1) {    //add any missing buckets in between existing ones
                last++;
                aggLogger.info(last + ELEMENT_SEP + 0);
            }
            last++;
            aggLogger.info(entry.getKey() + ELEMENT_SEP + entry.getValue());
        }
    }

    /**
     * Write a comment (with the comment prefix)
     *
     * @param comment the comment to write
     */
    private void writeComment(String comment) {
        aggLogger.info(COMMENT_PREFIX + " " + comment);
    }

    /**
     * Variables for aggregating for one interval
     */
    private class Stats implements Comparable {
        final int interval;
        //variables for aggregating
        long responseTimeTotal = 0L;
        long responseTimeCounter = 0L;
        long queueWaitTimeTotal = 0L;
        long queueWaitTimeCounter = 0L;
        int queueSize = 0;
        int queueSizeCounter = 0;
        long serviceTimeTotal = 0L;
        long serviceTimeCounter = 0L;
        int getCounter = 0;
        int multiCounter = 0;
        int setCounter = 0;
        int hitCounter = 0;
        int missCounter = 0;
        int noKeysCounter = 0;

        Stats(int interval) {
            this.interval = interval;
        }

        /**
         * Aggregate the collected data (i.e. calculate averages if necessary) and write it to the writer
         */
        void dump() {
            //calculate average where necessary (-1 if total is 0)
            long avgResponseTime = responseTimeTotal != 0 ? responseTimeTotal / responseTimeCounter : -1;
            long avgQueueWaitTime = queueWaitTimeTotal != 0 ? queueWaitTimeTotal / queueWaitTimeCounter : -1;
            long avgServiceTime = serviceTimeTotal != 0 ? serviceTimeTotal / serviceTimeCounter : -1;
            float avgNoKeys = (getCounter + multiCounter) != 0 ? ((float) noKeysCounter) / (getCounter + multiCounter) : -1;
            int avgQueueSize = queueSizeCounter != 0 ? queueSize / queueSizeCounter : 0;

            String line = interval + ELEMENT_SEP + avgResponseTime + ELEMENT_SEP + avgQueueWaitTime + ELEMENT_SEP +
                    avgQueueSize + ELEMENT_SEP + avgServiceTime + ELEMENT_SEP + getCounter + ELEMENT_SEP + multiCounter +
                    ELEMENT_SEP + setCounter + ELEMENT_SEP + hitCounter + ELEMENT_SEP + missCounter + ELEMENT_SEP +
                    avgNoKeys;
            aggLogger.info(line);
        }

        @Override
        public int compareTo(Object o) {
            return Integer.compare(interval, ((Stats) o).interval); //order by interval id
        }
    }
}
