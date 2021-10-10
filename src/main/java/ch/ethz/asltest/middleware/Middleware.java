package ch.ethz.asltest.middleware;

import ch.ethz.asltest.middleware.internal.NetThread;
import ch.ethz.asltest.middleware.internal.RequestQueue;
import ch.ethz.asltest.middleware.internal.ShutdownHook;
import ch.ethz.asltest.middleware.logging.StatsAggregator;
import ch.ethz.asltest.middleware.logging.StatsLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;


/**
 * The middleware object which contains all provided information like ip/port, server addresses, etc.
 * Clients send requests to middleware which it then handles. This includes parsing the request, sending it to the
 * correct server(s) and sending the response back to the client.
 */
public class Middleware {
    private static final Logger logger = LogManager.getLogger(Middleware.class);


    private final String ip;
    private final int port;
    private final List<String> mcAddresses;
    private final boolean readSharded;
    private final int numThreadsPTP;
    private final boolean log;

    private StatsLogger statsLogger;
    private NetThread netThread;
    private RequestQueue requestQueue;

    public Middleware(String ip, int port, List<String> mcAddresses, int numThreadsPTP, boolean readSharded) {
        this.ip = ip;
        this.port = port;
        this.mcAddresses = mcAddresses;
        this.readSharded = readSharded;
        this.numThreadsPTP = numThreadsPTP;
        this.log = true;    //true on default
    }

    public Middleware(String ip, int port, List<String> mcAddresses, int numThreadsPTP, boolean readSharded, boolean log) {
        this.ip = ip;
        this.port = port;
        this.mcAddresses = mcAddresses;
        this.readSharded = readSharded;
        this.numThreadsPTP = numThreadsPTP;
        this.log = log;
    }

    /**
     * Run the middleware, i.e. start the net and worker threads, initialize the request queue and handle logging.
     */
    public void run() {
        logger.info("starting middleware with the following parameters: " + this.toString());
        try {
            Runtime.getRuntime().addShutdownHook(new ShutdownHook(this));   //add the shutdown hook

            //get the request queue object
            requestQueue = RequestQueue.get();
            if (log) {
                //create the stats logger but don't start it yet, the net thread does that as soon as it accepts the first connection
                statsLogger = new StatsLogger();
            }
            //initialize and start the worker threads
            requestQueue.start(numThreadsPTP, mcAddresses, readSharded, log, statsLogger);
            logger.info("request queue and worker threads initialized/started");

            //start net thread
            netThread = new NetThread(ip, port, log, mcAddresses.size(), readSharded, statsLogger);
            netThread.start();
            logger.info("net thread started");
        } catch (Exception e) {
            logger.fatal("error while starting middleware", e);
            System.exit(1);
        }
    }

    /**
     * Shutdown the middleware. This includes stopping all threads and dumping logs.
     */
    public void shutdown() {
        logger.info("shutting down middleware");

        if (requestQueue != null) {
            logger.info("shutting down worker threads");
            requestQueue.shutdown();
        }

        if (netThread != null) {
            logger.info("shutting down net thread");
            netThread.interrupt();
            try {
                netThread.join();
            } catch (InterruptedException e) {
                logger.error("Interruption occurred while shutting down net thread", e);
            }
        }

        if (log && requestQueue != null && statsLogger != null) {
            logger.info("stopping log thread and aggregating/dumping logs. THIS MAY TAKE SOME TIME");
            statsLogger.interrupt();
            try {
                statsLogger.join();
            } catch (InterruptedException e) {
                logger.error("Interruption occurred while shutting down log thread", e);
            }
            aggregateLogs();
        } else {
            logger.info("logging disabled, no output");
        }

        logger.info("middleware shut down");
    }

    /**
     * Aggregate the logs in the raw.log file into aggregated.log and delete raw.log.
     */
    private void aggregateLogs() {
        try {
            StatsAggregator aggregator = new StatsAggregator();
            aggregator.aggregate();
        } catch (IOException e) {
            logger.error("something went wrong while aggregating logs", e);
        }
    }

    @Override
    public String toString() {
        return "Middleware{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", mcAddresses=" + mcAddresses +
                ", readSharded=" + readSharded +
                ", numThreadsPTP=" + numThreadsPTP +
                '}';
    }
}
