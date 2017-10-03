package com.digitalpebble.stormcrawler.fetching;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Tuple;

import com.digitalpebble.stormcrawler.Metadata;

public class TupleWrapper {

	private Tuple tuple;
	private TridentTuple tridentTuple;
	
	public TupleWrapper(Tuple tuple) {
		this.tuple = tuple;
	}

	public TupleWrapper(TridentTuple tuple) {
		this.tridentTuple = tuple; 
	}

	public String getStringByField(String string) {
		if ( this.tuple != null ) {
			return this.tuple.getStringByField(string);
		} else {
			// Not valid for Trident
			return "NOT VALID FOR TRIDENT";
		}
	}

	public Metadata getValueByField(String string) {
		if ( this.tuple != null ) {
			return (Metadata) this.tuple.getValueByField(string);
		} else {
			// Not valid for Trident
			return null;
		}
	}
	
	public byte[] getBinaryByField(String string) {
		if ( this.tuple != null ) {
			return this.tuple.getBinaryByField(string);
		} else {
			// Not valid for Trident
			return null;
		}
	}
	
	public boolean contains(String string) {
		if ( this.tuple != null ) {
			return this.tuple.contains(string);
		} else {
			// Not valid for Trident
			return false;
		}
	}
	
	public Tuple getTuple() {
		return this.tuple;
	}
	
}
