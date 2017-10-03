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

import java.net.URL;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.StatusEmitterBase;
import com.digitalpebble.stormcrawler.fetching.CallbackCollector;
import com.digitalpebble.stormcrawler.fetching.TupleWrapper;

/**
 * Provides common functionalities for Bolts which emit tuples to the status
 * stream, e.g. Fetchers, Parsers. Encapsulates the logic of URL filtering and
 * metadata transfer to outlinks.
 **/
public abstract class StatusEmitterBolt extends StatusEmitterBase implements IRichBolt, IComponent {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	super.prepare(stormConf, context, new CallbackCollector(collector));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                com.digitalpebble.stormcrawler.Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
    }

    @Override
    public void cleanup() {
    }   
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }   

    public void emitOutlink(Tuple input, URL url, String sitemapURL, 
    		Metadata metadata, String... customKeyVals) {
    	super.emitOutlink(new TupleWrapper(input), url, sitemapURL, metadata, 
    			customKeyVals);
	}
    
}
