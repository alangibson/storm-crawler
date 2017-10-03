package com.digitalpebble.stormcrawler.fetching;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt;
import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.parse.Outlink;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolFactory;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.protocol.RobotRules;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.digitalpebble.stormcrawler.util.PerSecondReducer;
import com.digitalpebble.stormcrawler.util.URLUtil;

import crawlercommons.robots.BaseRobotRules;

public class Fetcher {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(Fetcher.class);

    public static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

    // TODO need getters for these
    public AtomicInteger activeThreads = new AtomicInteger(0);
    public MultiCountMetric eventCounter;
    public FetchItemQueues fetchQueues;
    public String[] beingFetched;
    public boolean sitemapsAutoDiscovery = false;
    public ProtocolFactory protocolFactory;
    /** blocks the processing of new URLs if this value is reached **/
    public int maxNumberURLsInQueues = -1;
    
    private final AtomicInteger spinWaiting = new AtomicInteger(0);
    
    Config conf;
    private CallbackCollector collector;
    private boolean allowRedirs;
    
    // TODO copied from StatusEmitterBolt
    private URLFilters urlFilters;
    private MetadataTransfer metadataTransfer;
    
    // Metrics
    public MultiReducedMetric averagedMetrics;
    public MultiReducedMetric perSecMetrics;
    private int taskID = -1;
    
    public Fetcher(Config config, CallbackCollector collector, boolean allowRedirs, IMetricsContext metricsContext) {
    	this(config, allowRedirs, metricsContext);
        this.collector = collector;
    }

    public Fetcher(Config conf, boolean allowRedirs, IMetricsContext metricsContext) {
    	this.conf = conf;
        this.allowRedirs = allowRedirs;
        
        checkConfiguration(conf);
        
        protocolFactory = new ProtocolFactory(conf);

        this.fetchQueues = new FetchItemQueues(conf);
        
        int threadCount = ConfUtils.getInt(conf, "fetcher.threads.number", 10);
        for (int i = 0; i < threadCount; i++) { // spawn threads
            new FetcherThread(conf, i).start();
        }

        // keep track of the URLs in fetching
        this.beingFetched = new String[threadCount];
        Arrays.fill(this.beingFetched, "");

        // TODO
//        this.sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf,
//        		this.fetcher.SITEMAP_DISCOVERY_PARAM_KEY, false);
        this.sitemapsAutoDiscovery = false;

        // TODO copied from StatusEmitterBolt
        this.urlFilters = URLFilters.fromConf(conf);
        this.metadataTransfer = MetadataTransfer.getInstance(conf);
        // TODO just overwriting. remove argument above
        this.allowRedirs = ConfUtils.getBoolean(conf,
                com.digitalpebble.stormcrawler.Constants.AllowRedirParamName,
                true);
        
        maxNumberURLsInQueues = ConfUtils.getInt(conf,
                "fetcher.max.urls.in.queues", -1);

        //
        // Configure metrics
        //
        
        int metricsTimeBucketSecs = ConfUtils.getInt(conf,
                "fetcher.metrics.time.bucket.secs", 10);
        
        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = metricsContext.registerMetric("fetcher_counter",
                new MultiCountMetric(), metricsTimeBucketSecs);
        
        // create gauges
        metricsContext.registerMetric("activethreads", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return activeThreads.get();
            }
        }, metricsTimeBucketSecs);
        metricsContext.registerMetric("in_queues", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return fetchQueues.inQueues.get();
            }
        }, metricsTimeBucketSecs);
        metricsContext.registerMetric("num_queues", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return fetchQueues.queues.size();
            }
        }, metricsTimeBucketSecs);
        
        this.averagedMetrics = metricsContext.registerMetric("fetcher_average_perdoc",
                new MultiReducedMetric(new MeanReducer()),
                metricsTimeBucketSecs);
        
        this.perSecMetrics = metricsContext.registerMetric("fetcher_average_persec",
                new MultiReducedMetric(new PerSecondReducer()),
                metricsTimeBucketSecs);
        
    }
    
    private void checkConfiguration(Config conf) {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) conf.get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }
    
    public void execute(TupleWrapper input, String urlString, Metadata metadata) {
        boolean toomanyurlsinqueues = false;
        do {
            if (this.maxNumberURLsInQueues != -1
                    && (this.activeThreads.get() + this.fetchQueues.inQueues
                            .get()) >= this.maxNumberURLsInQueues) {
                toomanyurlsinqueues = true;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted exception caught in execute method");
                    Thread.currentThread().interrupt();
                }
            }
            LOG.info("[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}",
                    taskID, this.activeThreads.get(),
                    this.fetchQueues.queues.size(),
                    this.fetchQueues.inQueues.get());
        } while (toomanyurlsinqueues);

        // TODO
        // detect whether there is a file indicating that we should
        // dump the content of the queues to the log
//        if (debugfiletrigger != null && debugfiletrigger.exists()) {
//            LOG.info("Found trigger file {}", debugfiletrigger);
//            logQueuesContent();
//            debugfiletrigger.delete();
//        }

//        String urlString = input.getStringByField("url");
        URL url;

        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}",
                    taskID, input);
            // ignore silently
            collector.ack(input);
            return;
        }

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error("{} is a malformed URL", urlString);

//            Metadata metadata = (Metadata) input.getValueByField("metadata");
            if (metadata == null) {
                metadata = new Metadata();
            }
            // Report to status stream and ack
            metadata.setValue(Constants.STATUS_ERROR_CAUSE, "malformed URL");
            collector.emit(
                    com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                    input, new Values(urlString, metadata, Status.ERROR));
            collector.ack(input);
            return;
        }

        boolean added = this.fetchQueues.addFetchItem(url, input);
        if (!added) {
            collector.fail(input);
        }
    }

    /**
     * For Trident support.
     * 
     * @param tupleWrapper
     * @param callbackCollector
     */
	public void execute(TupleWrapper input, CallbackCollector callbackCollector, String urlString, Metadata metadata) {
		this.collector = callbackCollector;
		this.execute(input, urlString, metadata);
	}
    
    private void logQueuesContent() {
        StringBuilder sb = new StringBuilder();
        synchronized (this.fetchQueues.queues) {
            sb.append("\nNum queues : ").append(this.fetchQueues.queues.size());
            Iterator<Entry<String, FetchItemQueue>> iterator = this.fetchQueues.queues
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, FetchItemQueue> entry = iterator.next();
                sb.append("\nQueue ID : ").append(entry.getKey());
                FetchItemQueue fiq = entry.getValue();
                sb.append("\t size : ").append(fiq.getQueueSize());
                sb.append("\t in progress : ").append(fiq.getInProgressSize());
                Iterator<FetchItem> urlsIter = fiq.queue.iterator();
                while (urlsIter.hasNext()) {
                    sb.append("\n\t").append(urlsIter.next().url);
                }
            }
            LOG.info("Dumping queue content {}", sb.toString());

            StringBuilder sb2 = new StringBuilder("\n");
            // dump the list of URLs being fetched
            for (int i = 0; i < this.beingFetched.length; i++) {
                if (this.beingFetched[i].length() > 0) {
                    sb2.append("\n\tThread #").append(i).append(": ")
                            .append(this.beingFetched[i]);
                }
            }
            LOG.info("URLs being fetched {}", sb2.toString());
        }
    }


    /** Used for redirections or when discovering sitemap URLs **/
    protected void emitOutlink(TupleWrapper t, URL sURL, String newUrl,
            Metadata sourceMetadata, String... customKeyVals) {

        Outlink ol = filterOutlink(sURL, newUrl, sourceMetadata, customKeyVals);
        if (ol == null)
            return;

        collector.emit(
                com.digitalpebble.stormcrawler.Constants.StatusStreamName, 
                t,
                new Values(ol.getTargetURL(), ol.getMetadata(),
                        Status.DISCOVERED));
    }

    protected Outlink filterOutlink(URL sURL, String newUrl,
            Metadata sourceMetadata, String... customKeyVals) {
        // build an absolute URL
        try {
            URL tmpURL = URLUtil.resolveURL(sURL, newUrl);
            newUrl = tmpURL.toExternalForm();
        } catch (MalformedURLException e) {
            return null;
        }

        // apply URL filters
        newUrl = this.urlFilters.filter(sURL, sourceMetadata, newUrl);

        // filtered
        if (newUrl == null) {
            return null;
        }

        Metadata metadata = metadataTransfer.getMetaForOutlink(newUrl,
                sURL.toExternalForm(), sourceMetadata);

        for (int i = 0; i < customKeyVals.length; i = i + 2) {
            metadata.addValue(customKeyVals[i], customKeyVals[i + 1]);
        }

        Outlink l = new Outlink(newUrl);
        l.setMetadata(metadata);
        return l;
    }

    
    /**
     * This class picks items from queues and fetches the pages.
     */
    public class FetcherThread extends Thread {

        // longest delay accepted from robots.txt
        private final long maxCrawlDelay;
        private int threadNum;
       
        public FetcherThread(Config conf, int num) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("FetcherThread #" + num); // use an informative name

            this.maxCrawlDelay = ConfUtils.getInt(conf,
                    "fetcher.max.crawl.delay", 30) * 1000;
            this.threadNum = num;
        }

        @Override
        public void run() {
            while (true) {
                FetchItem fit = fetchQueues.getFetchItem();
                if (fit == null) {
                    LOG.debug("{} spin-waiting ...", getName());
                    // spin-wait.
                    spinWaiting.incrementAndGet();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.error("{} caught interrupted exception", getName());
                        Thread.currentThread().interrupt();
                    }
                    spinWaiting.decrementAndGet();
                    continue;
                }

                activeThreads.incrementAndGet(); // count threads

                beingFetched[threadNum] = fit.url;

                LOG.debug(
                        "[Fetcher #{}] {}  => activeThreads={}, spinWaiting={}, queueID={}",
                        taskID, getName(), activeThreads, spinWaiting,
                        fit.queueID);

                LOG.debug("[Fetcher #{}] {} : Fetching {}", taskID, getName(),
                        fit.url);

                Metadata metadata = null;

                if (fit.t.contains("metadata")) {
                    metadata = (Metadata) fit.t.getValueByField("metadata");
                }
                if (metadata == null) {
                    metadata = Metadata.empty;
                }

                boolean asap = false;

                try {
                    URL URL = new URL(fit.url);
                    Protocol protocol = protocolFactory.getProtocol(URL);

                    if (protocol == null)
                        throw new RuntimeException(
                                "No protocol implementation found for "
                                        + fit.url);

                    BaseRobotRules rules = protocol.getRobotRules(fit.url);
                    boolean fromCache = false;
                    if (rules instanceof RobotRules
                            && ((RobotRules) rules).getContentLengthFetched().length == 0) {
                        fromCache = true;
                        eventCounter.scope("robots.fromCache").incrBy(1);
                    } else {
                        eventCounter.scope("robots.fetched").incrBy(1);
                    }

                    // autodiscovery of sitemaps
                    // the sitemaps will be sent down the topology
                    // as many times as there is a URL for a given host
                    // the status updater will certainly cache things
                    // but we could also have a simple cache mechanism here
                    // as well
                    // if the robot come from the cache there is no point
                    // in sending the sitemap URLs again

                    // check in the metadata if discovery setting has been
                    // overridden
                    boolean smautodisco = sitemapsAutoDiscovery;
                    String localSitemapDiscoveryVal = metadata
                            .getFirstValue(SITEMAP_DISCOVERY_PARAM_KEY);
                    if ("true".equalsIgnoreCase(localSitemapDiscoveryVal)) {
                        smautodisco = true;
                    } else if ("false"
                            .equalsIgnoreCase(localSitemapDiscoveryVal)) {
                        smautodisco = false;
                    }

                    if (!fromCache && smautodisco) {
                        for (String sitemapURL : rules.getSitemaps()) {
                            emitOutlink(fit.t, URL, sitemapURL, metadata,
                                    SiteMapParserBolt.isSitemapKey, "true");
                        }
                    }

                    if (!rules.isAllowed(fit.u.toString())) {
                        LOG.info("Denied by robots.txt: {}", fit.url);
                        // pass the info about denied by robots
                        metadata.setValue(Constants.STATUS_ERROR_CAUSE,
                                "robots.txt");
                        collector
                                .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                        fit.t, new Values(fit.url, metadata,
                                                Status.ERROR));
                        // no need to wait next time as we won't request from
                        // that site
                        asap = true;
                        continue;
                    }
                    if (rules.getCrawlDelay() > 0) {
                        if (rules.getCrawlDelay() > maxCrawlDelay
                                && maxCrawlDelay >= 0) {
                            LOG.info(
                                    "Crawl-Delay for {} too long ({}), skipping",
                                    fit.url, rules.getCrawlDelay());
                            // pass the info about crawl delay
                            metadata.setValue(Constants.STATUS_ERROR_CAUSE,
                                    "crawl_delay");
                            collector
                                    .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                            fit.t, new Values(fit.url,
                                                    metadata, Status.ERROR));
                            // no need to wait next time as we won't request
                            // from that site
                            asap = true;
                            continue;
                        } else {
                            FetchItemQueue fiq = fetchQueues
                                    .getFetchItemQueue(fit.queueID);
                            fiq.crawlDelay = rules.getCrawlDelay();
                            LOG.info(
                                    "Crawl delay for queue: {}  is set to {} as per robots.txt. url: {}",
                                    fit.queueID, fiq.crawlDelay, fit.url);
                        }
                    }

                    long start = System.currentTimeMillis();
                    ProtocolResponse response = protocol.getProtocolOutput(
                            fit.url, metadata);
                    long timeFetching = System.currentTimeMillis() - start;

                    final int byteLength = response.getContent().length;

                    averagedMetrics.scope("fetch_time").update(timeFetching);
                    averagedMetrics.scope("bytes_fetched").update(byteLength);
                    perSecMetrics.scope("bytes_fetched_perSec").update(
                            byteLength);
                    perSecMetrics.scope("fetched_perSec").update(1);
                    eventCounter.scope("fetched").incrBy(1);
                    eventCounter.scope("bytes_fetched").incrBy(byteLength);

                    LOG.info(
                            "[Fetcher #{}] Fetched {} with status {} in msec {}",
                            taskID, fit.url, response.getStatusCode(),
                            timeFetching);

                    // passes the input metadata if any to the response one
                    response.getMetadata().putAll(metadata);

                    response.getMetadata().setValue("fetch.statusCode",
                            Integer.toString(response.getStatusCode()));

                    response.getMetadata().setValue("fetch.loadingTime",
                            Long.toString(timeFetching));

                    // determine the status based on the status code
                    final Status status = Status.fromHTTPCode(response
                            .getStatusCode());

                    final Values tupleToSend = new Values(fit.url,
                            response.getMetadata(), status);

                    // if the status is OK emit on default stream
                    if (status.equals(Status.FETCHED)) {
                        if (response.getStatusCode() == 304) {
                            // mark this URL as fetched so that it gets
                            // rescheduled
                            // but do not try to parse or index
                            collector.emit(Constants.StatusStreamName, fit.t,
                                    tupleToSend);
                        } else {
                            // send content for parsing
                            collector.emit(fit.t,
                                    new Values(fit.url, response.getContent(),
                                            response.getMetadata()));
                        }
                    } else if (status.equals(Status.REDIRECTION)) {

                        // find the URL it redirects to
                        String redirection = response.getMetadata()
                                .getFirstValue(HttpHeaders.LOCATION);

                        // stores the URL it redirects to
                        // used for debugging mainly - do not resolve the target
                        // URL
                        if (StringUtils.isNotBlank(redirection)) {
                            response.getMetadata().setValue("_redirTo",
                                    redirection);
                        }

                        // mark this URL as redirected
                        collector.emit(Constants.StatusStreamName, fit.t,
                                tupleToSend);

                        if (allowRedirs
                                && StringUtils.isNotBlank(redirection)) {
                            emitOutlink(fit.t, URL, redirection,
                                    response.getMetadata());
                        }
                    }
                    // error
                    else {
                        collector.emit(Constants.StatusStreamName, fit.t,
                                tupleToSend);
                    }

                } catch (Exception exece) {
                    String message = exece.getMessage();
                    if (message == null)
                        message = "";

                    // common exceptions for which we log only a short message
                    if (exece.getCause() instanceof java.util.concurrent.TimeoutException
                            || message.contains(" timed out")) {
                        LOG.error("Socket timeout fetching {}", fit.url);
                        message = "Socket timeout fetching";
                    } else if (exece.getCause() instanceof java.net.UnknownHostException
                            || exece instanceof java.net.UnknownHostException) {
                        LOG.error("Unknown host {}", fit.url);
                        message = "Unknown host";
                    } else {
                        LOG.error("Exception while fetching {}", fit.url, exece);
                        message = exece.getClass().getName();
                    }

                    if (metadata.size() == 0) {
                        metadata = new Metadata();
                    }
                    // add the reason of the failure in the metadata
                    metadata.setValue("fetch.exception", message);

                    // send to status stream
                    collector.emit(Constants.StatusStreamName, fit.t,
                            new Values(fit.url, metadata, Status.FETCH_ERROR));

                    eventCounter.scope("exception").incrBy(1);
                } finally {
                    fetchQueues.finishFetchItem(fit, asap);
                    activeThreads.decrementAndGet(); // count threads
                    // ack it whatever happens
                    collector.ack(fit.t);
                    beingFetched[threadNum] = "";
                }
            }
        }
    }
	
}
