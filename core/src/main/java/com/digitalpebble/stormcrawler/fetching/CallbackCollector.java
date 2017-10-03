package com.digitalpebble.stormcrawler.fetching;

import java.util.Collection;
import java.util.List;

import org.apache.storm.shade.org.eclipse.jetty.util.log.Log;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.parse.JSoupParser;

// TODO use delegate construtor arg of OutputCollector instead?
/**
 * Trident portions emit union of all possible tuple values:
 *   stream, url, metadata, status, content, text
 * because:
 * JSoupParser emits 2 streams
 *   default = url, content, metadata, text
 *   status  = url, metadata, status
 * FeedParser emits 2 streams:
 *   default = url, content, metadata
 *   status  = url, metadata, status
 */
public class CallbackCollector extends OutputCollector {

	private static final Logger log = LoggerFactory
            .getLogger(CallbackCollector.class);
	
	private OutputCollector stormCollector;
	private TridentCollector tridentCollector;
	
	public CallbackCollector(OutputCollector collector) {
		super(collector);
		// TODO get rid of stormCollector since it is our delegate
		this.stormCollector = collector;
	}
	
	public CallbackCollector(TridentCollector tridentCollector) {
		super(null);
		this.tridentCollector = tridentCollector;
	}

	public void ack(TupleWrapper tuple) {
		if ( this.stormCollector != null ) {
			this.stormCollector.ack(tuple.getTuple());
		} // else: do nothing for TridentCollector
	}
	
	public void fail(TupleWrapper tuple) {
		if ( this.stormCollector != null ) {
			this.stormCollector.fail(tuple.getTuple());
		} // else: do nothing for TridentCollector
	}
	
	public void emit(TupleWrapper tuple, Values values) {
		if ( this.stormCollector != null ) {
			this.stormCollector.emit(tuple.getTuple(), values);
		} else {
			log.debug("Incoming values: {}", values);
			Values unionValues = new Values(
					"default",		// streamId
					values.size() >= 1 ? values.get(0) : null,	
									// url
					values.size() >= 3 ? values.get(2) : null,	
									// metadata 
					null,			// status 
					values.size() >= 2 ? values.get(1) : null, 	
									// content
					values.size() >= 4 ? values.get(3) : null 	
									// text
				); 
			log.debug("Unified values: {}", unionValues);
			this.tridentCollector.emit(unionValues);
		}
	}
	
	/**
	 * Emit Tuple to status stream.
	 * @param streamId
	 * @param tuple
	 * @param values
	 */
	public void emit(String streamId, TupleWrapper tuple, Values values) {
		if ( this.stormCollector != null ) {
			this.stormCollector.emit(streamId, tuple.getTuple(), values);
		} else {
			this.tridentCollector.emit(
				new Values(
					streamId,
					values.get(0),	// url
					values.get(1),	// metadata 
					values.get(2),	// status 
					null, 			// content
					null 			// text
				) );
		}
	}

	//
	// IOutputCollector
	//
	
	@Override
	public void reportError(Throwable error) {
		if ( this.stormCollector != null ) {
			this.stormCollector.reportError(error);
		} else {
			this.tridentCollector.reportError(error);
		}
	}

	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		if ( this.stormCollector != null ) {
			return this.stormCollector.emit(streamId, anchors, tuple);
		} else {
			// TODO normalize values if we ever actually get here
			this.tridentCollector.emit(tuple);
			return null;
		}
	}

	@Override
	public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		if ( this.stormCollector != null ) {
			this.stormCollector.emitDirect(taskId, streamId, anchors, tuple);
		} else {
			this.tridentCollector.emit(tuple);
		}
	}

	@Override
	public void ack(Tuple input) {
		if ( this.stormCollector != null ) {
			this.stormCollector.ack(input);
		} 
	}

	@Override
	public void fail(Tuple input) {
		if ( this.stormCollector != null ) {
			this.stormCollector.fail(input);
		}
	}

	@Override
	public void resetTimeout(Tuple input) {
		if ( this.stormCollector != null ) {
			this.stormCollector.resetTimeout(input);
		}
	}
	
}
