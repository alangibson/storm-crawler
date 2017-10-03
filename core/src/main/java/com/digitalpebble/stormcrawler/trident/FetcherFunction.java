package com.digitalpebble.stormcrawler.trident;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.fetching.CallbackCollector;
import com.digitalpebble.stormcrawler.fetching.Fetcher;
import com.digitalpebble.stormcrawler.fetching.TupleWrapper;

public class FetcherFunction implements Function {

	private Fetcher fetcher;

	@Override
	public void prepare(Map tridentConf, TridentOperationContext context) {
		// TODO 
		boolean allowRedirs = false;
		
		Config conf = new Config();
        conf.putAll(tridentConf);

        this.fetcher = new Fetcher(conf, allowRedirs, context);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String urlString = tuple.getString(0); 
//		Metadata metadata = (Metadata) tuple.get(1);
		Metadata metadata = null;
		this.fetcher.execute(new TupleWrapper(tuple), new CallbackCollector(collector), urlString, metadata);
	}

	@Override
	public void cleanup() {
		this.fetcher.protocolFactory.cleanup();
	}

}
