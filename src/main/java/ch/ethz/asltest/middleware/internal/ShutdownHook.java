package ch.ethz.asltest.middleware.internal;

import ch.ethz.asltest.middleware.Middleware;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The Shutdown Hook which is invoked when the JVM is terminated (e.g. by pressing CTRL+C)
 */
public class ShutdownHook extends Thread {

    private static final Logger logger = LogManager.getLogger(ShutdownHook.class);
    private Middleware mw = null;

    public ShutdownHook(Middleware mw) {
        this.mw = mw;
    }

    @Override
    public void run() {
        if (mw != null) {
            logger.info("hook is shutting down middleware");
            mw.shutdown();
            logger.info("hook shut down middleware successfully");
            LogManager.shutdown();  //needs to be manually shut down
        }
    }
}
