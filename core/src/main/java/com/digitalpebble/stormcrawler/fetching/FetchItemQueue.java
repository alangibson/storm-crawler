package com.digitalpebble.stormcrawler.fetching;

import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.LoggerFactory;

/**
 * This class handles FetchItems which come from the same host ID (be it a
 * proto/hostname or proto/IP pair). It also keeps track of requests in progress
 * and elapsed time between requests.
 */
public class FetchItemQueue {
	
	private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(FetchItemQueue.class);
	
	AtomicInteger inProgress = new AtomicInteger();
	AtomicLong nextFetchTime = new AtomicLong();

	// TODO need mutators for these
	public Deque<FetchItem> queue = new LinkedBlockingDeque<>();
	public long crawlDelay;
	
	final long minCrawlDelay;
	final int maxThreads;

	public FetchItemQueue(int maxThreads, long crawlDelay, long minCrawlDelay) {
		this.maxThreads = maxThreads;
		this.crawlDelay = crawlDelay;
		this.minCrawlDelay = minCrawlDelay;
		// ready to start
		setEndTime(System.currentTimeMillis() - crawlDelay);
	}

	public int getQueueSize() {
		return queue.size();
	}

	public int getInProgressSize() {
		return inProgress.get();
	}

	public void finishFetchItem(FetchItem it, boolean asap) {
		if (it != null) {
			inProgress.decrementAndGet();
			setEndTime(System.currentTimeMillis(), asap);
		}
	}

	public void addFetchItem(FetchItem it) {
		queue.add(it);
	}

	public FetchItem getFetchItem() {
		if (inProgress.get() >= maxThreads)
			return null;
		long now = System.currentTimeMillis();
		if (nextFetchTime.get() > now)
			return null;
		FetchItem it = null;
		if (queue.isEmpty())
			return null;
		try {
			it = queue.removeFirst();
			inProgress.incrementAndGet();
		} catch (Exception e) {
			LOG.error("Cannot remove FetchItem from queue or cannot add it to inProgress queue", e);
		}
		return it;
	}

	private void setEndTime(long endTime) {
		setEndTime(endTime, false);
	}

	private void setEndTime(long endTime, boolean asap) {
		if (!asap)
			nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
		else
			nextFetchTime.set(endTime);
	}

}
