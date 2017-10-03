/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.bolt;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.fetching.CallbackCollector;
import com.digitalpebble.stormcrawler.fetching.TupleWrapper;
import com.digitalpebble.stormcrawler.parse.JSoupParser;

/**
 * Parser for HTML documents only which uses ICU4J to detect the charset
 * encoding. Kindly donated to storm-crawler by shopstyle.com.
 */
@SuppressWarnings("serial")
public class JSoupParserBolt extends StatusEmitterBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(JSoupParserBolt.class);

    private JSoupParser parser;
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(conf, context, collector);

        Config config = new Config();
        config.putAll(conf);
        
        CallbackCollector callbackCollector = new CallbackCollector(collector);
        this.parser = new JSoupParser(config, callbackCollector, context);
    }

    @Override
    public void execute(Tuple tuple) {
        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
    	this.parser.execute(new TupleWrapper(tuple), content, url, metadata);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        // output of this module is the list of fields to index
        // with at least the URL, text content
        declarer.declare(new Fields("url", "content", "metadata", "text"));
    }


}
