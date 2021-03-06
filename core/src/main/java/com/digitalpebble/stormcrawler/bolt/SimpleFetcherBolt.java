/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.bolt;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolFactory;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.protocol.RobotRules;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.PerSecondReducer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import crawlercommons.domains.PaidLevelDomain;
import crawlercommons.robots.BaseRobotRules;

/**
 * A single-threaded fetcher with no internal queue. Use of this fetcher
 * requires that the user implement an external queue that enforces crawl-delay
 * politeness constraints.
 */
@SuppressWarnings("serial")
public class SimpleFetcherBolt extends StatusEmitterBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(SimpleFetcherBolt.class);

    private static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

    public static final String QUEUE_MODE_HOST = "byHost";
    public static final String QUEUE_MODE_DOMAIN = "byDomain";
    public static final String QUEUE_MODE_IP = "byIP";

    private Config conf;

    private MultiCountMetric eventCounter;
    private MultiReducedMetric averagedMetrics;
    private MultiReducedMetric perSecMetrics;

    private ProtocolFactory protocolFactory;

    private int taskID = -1;

    boolean sitemapsAutoDiscovery = false;

    // TODO configure the max time
    private Cache<String, Long> throttler = CacheBuilder.newBuilder()
            .expireAfterAccess(30, TimeUnit.SECONDS).build();

    private String queueMode;

    /** default crawl delay in msec, can be overridden by robots directives **/
    private long crawlDelay = 1000;

    /** max value accepted from robots.txt **/
    private long maxCrawlDelay = 30000;

    private void checkConfiguration() {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) getConf().get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private Config getConf() {
        return this.conf;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.conf = new Config();
        this.conf.putAll(stormConf);

        checkConfiguration();

        this.taskID = context.getThisTaskId();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.ENGLISH);
        long start = System.currentTimeMillis();
        LOG.info("[Fetcher #{}] : starting at {}", taskID, sdf.format(start));

        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology

        int metricsTimeBucketSecs = ConfUtils.getInt(conf,
                "fetcher.metrics.time.bucket.secs", 10);

        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), metricsTimeBucketSecs);

        this.averagedMetrics = context.registerMetric("fetcher_average",
                new MultiReducedMetric(new MeanReducer()),
                metricsTimeBucketSecs);

        this.perSecMetrics = context.registerMetric("fetcher_average_persec",
                new MultiReducedMetric(new PerSecondReducer()),
                metricsTimeBucketSecs);

        context.registerMetric("throttler_size", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return throttler.size();
            }
        }, metricsTimeBucketSecs);

        protocolFactory = new ProtocolFactory(conf);

        sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf,
                SITEMAP_DISCOVERY_PARAM_KEY, false);

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

        this.maxCrawlDelay = (long) ConfUtils.getInt(conf,
                "fetcher.max.crawl.delay", 30) * 1000;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    @Override
    public void cleanup() {
        protocolFactory.cleanup();
    }

    @Override
    public void execute(Tuple input) {

        String urlString = input.getStringByField("url");
        if (StringUtils.isBlank(urlString)) {
            LOG.info("[Fetcher #{}] Missing value for field url in tuple {}",
                    taskID, input);
            // ignore silently
            collector.ack(input);
            return;
        }

        Metadata metadata = null;

        if (input.contains("metadata"))
            metadata = (Metadata) input.getValueByField("metadata");
        if (metadata == null)
            metadata = Metadata.empty;

        URL url;

        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOG.error("{} is a malformed URL", urlString);
            // Report to status stream and ack
            if (metadata == Metadata.empty) {
                metadata = new Metadata();
            }
            metadata.setValue(Constants.STATUS_ERROR_CAUSE, "malformed URL");
            collector.emit(
                    com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                    input, new Values(urlString, metadata, Status.ERROR));
            collector.ack(input);
            return;
        }

        String key = getPolitenessKey(url);
        long delay = 0;

        try {
            Protocol protocol = protocolFactory.getProtocol(url);

            BaseRobotRules rules = protocol.getRobotRules(urlString);
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
            // as well.
            // if the robot come from the cache there is no point
            // in sending the sitemap URLs again

            // check in the metadata if discovery setting has been
            // overridden
            boolean smautodisco = sitemapsAutoDiscovery;
            String localSitemapDiscoveryVal = metadata
                    .getFirstValue(SITEMAP_DISCOVERY_PARAM_KEY);
            if ("true".equalsIgnoreCase(localSitemapDiscoveryVal)) {
                smautodisco = true;
            } else if ("false".equalsIgnoreCase(localSitemapDiscoveryVal)) {
                smautodisco = false;
            }

            if (!fromCache && smautodisco) {
                for (String sitemapURL : rules.getSitemaps()) {
                    emitOutlink(input, url, sitemapURL, metadata,
                            SiteMapParserBolt.isSitemapKey, "true");
                }
            }

            if (!rules.isAllowed(urlString)) {
                LOG.info("Denied by robots.txt: {}", urlString);

                metadata.setValue(Constants.STATUS_ERROR_CAUSE, "robots.txt");

                // Report to status stream and ack
                collector
                        .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                input, new Values(urlString, metadata,
                                        Status.ERROR));
                collector.ack(input);
                return;
            }

            // check when we are allowed to process it
            long timeWaiting = 0;

            Long timeAllowed = throttler.getIfPresent(key);

            if (timeAllowed != null) {
                long now = System.currentTimeMillis();
                long timeToWait = timeAllowed - now;
                if (timeToWait > 0) {
                    timeWaiting = timeToWait;
                    try {
                        Thread.sleep(timeToWait);
                    } catch (InterruptedException e) {
                        LOG.error("[Fetcher #{}] caught InterruptedException caught while waiting");
                        Thread.currentThread().interrupt();
                    }
                }
            }

            delay = this.crawlDelay;

            // get the delay from robots
            // value is negative when not set
            long robotsDelay = rules.getCrawlDelay();
            if (robotsDelay > 0) {
                // cap the value to a maximum
                // as some sites specify ridiculous values
                if (robotsDelay > maxCrawlDelay) {
                    LOG.debug("Delay from robots capped at {} for {}",
                            robotsDelay, url);
                    delay = maxCrawlDelay;
                } else {
                    delay = robotsDelay;
                }
            }

            LOG.debug("[Fetcher #{}] : Fetching {}", taskID, urlString);

            long start = System.currentTimeMillis();
            ProtocolResponse response = protocol.getProtocolOutput(urlString,
                    metadata);
            long timeFetching = System.currentTimeMillis() - start;

            final int byteLength = response.getContent().length;

            averagedMetrics.scope("wait_time").update(timeWaiting);
            averagedMetrics.scope("fetch_time").update(timeFetching);
            averagedMetrics.scope("bytes_fetched").update(byteLength);
            eventCounter.scope("fetched").incrBy(1);
            eventCounter.scope("bytes_fetched").incrBy(byteLength);
            perSecMetrics.scope("bytes_fetched_perSec").update(byteLength);
            perSecMetrics.scope("fetched_perSec").update(1);

            LOG.info(
                    "[Fetcher #{}] Fetched {} with status {} in {} after waiting {}",
                    taskID, urlString, response.getStatusCode(), timeFetching,
                    timeWaiting);

            response.getMetadata().putAll(metadata);

            response.getMetadata().setValue("fetch.statusCode",
                    Integer.toString(response.getStatusCode()));

            response.getMetadata().setValue("fetch.loadingTime",
                    Long.toString(timeFetching));

            // determine the status based on the status code
            final Status status = Status.fromHTTPCode(response.getStatusCode());

            // used when sending to status stream
            final Values values4status = new Values(urlString,
                    response.getMetadata(), status);

            // if the status is OK emit on default stream
            if (status.equals(Status.FETCHED)) {
                if (response.getStatusCode() == 304) {
                    // mark this URL as fetched so that it gets
                    // rescheduled
                    // but do not try to parse or index
                    collector
                            .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                    input, values4status);
                } else {
                    collector.emit(Utils.DEFAULT_STREAM_ID, input,
                            new Values(urlString, response.getContent(),
                                    response.getMetadata()));
                }
            } else if (status.equals(Status.REDIRECTION)) {

                // find the URL it redirects to
                String redirection = response.getMetadata().getFirstValue(
                        HttpHeaders.LOCATION);

                // stores the URL it redirects to
                // used for debugging mainly - do not resolve the target
                // URL
                if (StringUtils.isNotBlank(redirection)) {
                    response.getMetadata().setValue("_redirTo", redirection);
                }

                if (allowRedirs() && StringUtils.isNotBlank(redirection)) {
                    emitOutlink(input, url, redirection, response.getMetadata());
                }
                // Mark URL as redirected
                collector
                        .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                input, values4status);
            } else {
                // Error
                collector
                        .emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                                input, values4status);
            }

        } catch (Exception exece) {

            String message = exece.getMessage();
            if (message == null)
                message = "";

            // common exceptions for which we log only a short message
            if (exece.getCause() instanceof java.util.concurrent.TimeoutException
                    || message.contains(" timed out")) {
                LOG.error("Socket timeout fetching {}", urlString);
                message = "Socket timeout fetching";
            } else if (exece.getCause() instanceof java.net.UnknownHostException
                    || exece instanceof java.net.UnknownHostException) {
                LOG.error("Unknown host {}", urlString);
                message = "Unknown host";
            } else {
                LOG.error("Exception while fetching {}", urlString, exece);
                message = exece.getClass().getName();
            }
            eventCounter.scope("exception").incrBy(1);

            // could be an empty, immutable Metadata
            if (metadata.size() == 0) {
                metadata = new Metadata();
            }

            // add the reason of the failure in the metadata
            metadata.setValue("fetch.exception", message);

            collector.emit(
                    com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                    input, new Values(urlString, metadata, Status.FETCH_ERROR));
        }

        // update the throttler
        throttler.put(key, System.currentTimeMillis() + delay);

        collector.ack(input);
    }

	private String getPolitenessKey(URL u) {
        String key;
        if (QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
            try {
                final InetAddress addr = InetAddress.getByName(u.getHost());
                key = addr.getHostAddress();
            } catch (final UnknownHostException e) {
                // unable to resolve it, so don't fall back to host name
                LOG.warn("Unable to resolve: {}, skipping.", u.getHost());
                return null;
            }
        } else if (QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
            key = PaidLevelDomain.getPLD(u.getHost());
            if (key == null) {
                LOG.warn("Unknown domain for url: {}, using hostname as key",
                        u.toExternalForm());
                key = u.getHost();
            }
        } else {
            key = u.getHost();
            if (key == null) {
                LOG.warn("Unknown host for url: {}, using URL string as key",
                        u.toExternalForm());
                key = u.toExternalForm();
            }
        }
        return key.toLowerCase(Locale.ROOT);
    }

}
