package com.digitalpebble.stormcrawler.fetching;

import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Convenience class - a collection of queues that keeps track of the total
 * number of items, and provides items eligible for fetching from any queue.
 */
public class FetchItemQueues {
	
	private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(FetchItemQueues.class);
	
    // TODO need mutators for these
	public Map<String, FetchItemQueue> queues = Collections
            .synchronizedMap(new LinkedHashMap<String, FetchItemQueue>());
    public AtomicInteger inQueues = new AtomicInteger(0);

    final int defaultMaxThread;
    final long crawlDelay;
    final long minCrawlDelay;

    final int maxQueueSize;

    final Config conf;

    public static final String QUEUE_MODE_HOST = "byHost";
    public static final String QUEUE_MODE_DOMAIN = "byDomain";
    public static final String QUEUE_MODE_IP = "byIP";

    String queueMode;

    public FetchItemQueues(Config conf) {
        this.conf = conf;
        this.defaultMaxThread = ConfUtils.getInt(conf,
                "fetcher.threads.per.queue", 1);
        queueMode = ConfUtils.getString(conf, "fetcher.queue.mode",
                QUEUE_MODE_HOST);
        // check that the mode is known
        if (!queueMode.equals(QUEUE_MODE_IP)
                && !queueMode.equals(QUEUE_MODE_DOMAIN)
                && !queueMode.equals(QUEUE_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost",
                    queueMode);
            queueMode = QUEUE_MODE_HOST;
        }
        LOG.info("Using queue mode : {}", queueMode);

        this.crawlDelay = (long) (ConfUtils.getFloat(conf,
                "fetcher.server.delay", 1.0f) * 1000);
        this.minCrawlDelay = (long) (ConfUtils.getFloat(conf,
                "fetcher.server.min.delay", 0.0f) * 1000);
        this.maxQueueSize = ConfUtils.getInt(conf,
                "fetcher.max.queue.size", -1);
    }

    public synchronized boolean addFetchItem(URL u, TupleWrapper input) {
        FetchItem it = FetchItem.create(u, input, queueMode);
        FetchItemQueue fiq = getFetchItemQueue(it.queueID);
        if (maxQueueSize > 0 && fiq.getQueueSize() >= maxQueueSize) {
            return false;
        }
        fiq.addFetchItem(it);
        inQueues.incrementAndGet();
        return true;
    }

    public synchronized void finishFetchItem(FetchItem it, boolean asap) {
        FetchItemQueue fiq = queues.get(it.queueID);
        if (fiq == null) {
            LOG.warn("Attempting to finish item from unknown queue: {}",
                    it.queueID);
            return;
        }
        fiq.finishFetchItem(it, asap);
    }

    public synchronized FetchItemQueue getFetchItemQueue(String id) {
        FetchItemQueue fiq = queues.get(id);
        if (fiq == null) {
            // custom maxThread value?
            final int customThreadVal = ConfUtils.getInt(conf,
                    "fetcher.maxThreads." + id, defaultMaxThread);
            // initialize queue
            fiq = new FetchItemQueue(customThreadVal, crawlDelay,
                    minCrawlDelay);
            queues.put(id, fiq);
        }
        return fiq;
    }

    public synchronized FetchItem getFetchItem() {
        if (queues.isEmpty()) {
            return null;
        }

        FetchItemQueue start = null;

        do {
            Iterator<Entry<String, FetchItemQueue>> i = queues.entrySet()
                    .iterator();

            if (!i.hasNext()) {
                return null;
            }

            Map.Entry<String, FetchItemQueue> nextEntry = i.next();

            if (nextEntry == null) {
                return null;
            }

            FetchItemQueue fiq = nextEntry.getValue();

            // We remove the entry and put it at the end of the map
            i.remove();

            // reap empty queues
            if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
                continue;
            }

            // Put the entry at the end no matter the result
            queues.put(nextEntry.getKey(), nextEntry.getValue());

            // In case of we are looping
            if (start == null) {
                start = fiq;
            } else if (fiq == start) {
                return null;
            }

            FetchItem fit = fiq.getFetchItem();

            if (fit != null) {
                inQueues.decrementAndGet();
                return fit;
            }

        } while (!queues.isEmpty());

        return null;
    }
}

