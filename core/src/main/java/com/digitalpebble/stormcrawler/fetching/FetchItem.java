package com.digitalpebble.stormcrawler.fetching;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import crawlercommons.domains.PaidLevelDomain;

/**
 * This class described the item to be fetched.
 */
public class FetchItem {

	private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(FetchItem.class);
	
	// TODO need mutators for these
	public String queueID;
	public String url;
	public URL u;
	public TupleWrapper t;

	public FetchItem(String url, URL u, TupleWrapper t, String queueID) {
		this.url = url;
		this.u = u;
		this.queueID = queueID;
		this.t = t;
	}

	/**
	 * Create an item. Queue id will be created based on <code>queueMode</code>
	 * argument, either as a protocol + hostname pair, protocol + IP address pair or
	 * protocol+domain pair.
	 */

	public static FetchItem create(URL u, TupleWrapper t, String queueMode) {

		String queueID;

		String url = u.toExternalForm();

		String key = null;
		// reuse any key that might have been given
		// be it the hostname, domain or IP
		if (t.contains("key")) {
			key = t.getStringByField("key");
		}
		if (StringUtils.isNotBlank(key)) {
			queueID = key.toLowerCase(Locale.ROOT);
			return new FetchItem(url, u, t, queueID);
		}

		if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
			try {
				final InetAddress addr = InetAddress.getByName(u.getHost());
				key = addr.getHostAddress();
			} catch (final UnknownHostException e) {
				LOG.warn("Unable to resolve IP for {}, using hostname as key.", u.getHost());
				key = u.getHost();
			}
		} else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
			key = PaidLevelDomain.getPLD(u.getHost());
			if (key == null) {
				LOG.warn("Unknown domain for url: {}, using hostname as key", url);
				key = u.getHost();
			}
		} else {
			key = u.getHost();
		}

		if (key == null) {
			LOG.warn("Unknown host for url: {}, using URL string as key", url);
			key = u.toExternalForm();
		}

		queueID = key.toLowerCase(Locale.ROOT);
		return new FetchItem(url, u, t, queueID);
	}

}
