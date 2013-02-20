package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;


public class HealthChecker implements Runnable {

    private boolean running = true;
    private ESLogger logger;
    private ElasticSearchCAPIBehavior capiBehavior;
    private ElasticSearchCouchbaseBehavior couchbaseBehavior;
    private long checkInterval;
    private long threshold;

    public HealthChecker(long checkInterval, long threshold, ESLogger logger, ElasticSearchCouchbaseBehavior couchbaseBehavior, ElasticSearchCAPIBehavior capiBehavior) {
        this.couchbaseBehavior = couchbaseBehavior;
        this.capiBehavior = capiBehavior;
        this.checkInterval = checkInterval;
        this.threshold = threshold;
        this.logger = logger;
    }

    @Override
    public void run() {
        while(running) {
            try {
                Thread.sleep(checkInterval);

                long start = System.currentTimeMillis();
                couchbaseBehavior.getBucketsInPool("default");
                long end = System.currentTimeMillis();

                long took = end - start;
                if(took > threshold) {
                    logger.info("Health check took {} exceeding threshold {}, marking service unavailable.", took, threshold);
                    couchbaseBehavior.setAvailable(false);
                    capiBehavior.setAvailable(false);
                } else {
                    logger.info("Health check passed, took {}", took);
                    couchbaseBehavior.setAvailable(true);
                    capiBehavior.setAvailable(true);
                }


            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    public void stop() {
        running = false;
    }

}
