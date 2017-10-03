package com.digitalpebble.stormcrawler.trident;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.fetching.CallbackCollector;
import com.digitalpebble.stormcrawler.fetching.TupleWrapper;
import com.digitalpebble.stormcrawler.parse.JSoupParser;

public class JSoupParserFunction implements Function {

	private JSoupParser parser;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		Config config = new Config();
        config.putAll(conf);
        this.parser = new JSoupParser(config, context);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		CallbackCollector callbackCollector = new CallbackCollector(collector);
		// TODO handle status stream?
		// stream, url, metadata, status, content, text
		String url = tuple.getString(1);
		Metadata metadata = (Metadata) tuple.get(2);
		byte[] content = tuple.getBinary(4);
		this.parser.execute(new TupleWrapper(tuple), callbackCollector, content, url, metadata);
	}

	@Override
	public void cleanup() {
	}

}
