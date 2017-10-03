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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.fetching.CallbackCollector;
import com.digitalpebble.stormcrawler.fetching.Fetcher;
import com.digitalpebble.stormcrawler.fetching.TupleWrapper;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the
 * politeness and handles the fetching threads itself.
 */
@SuppressWarnings("serial")
public class FetcherBolt extends StatusEmitterBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(FetcherBolt.class);

    private Fetcher fetcher;
    
    private int taskID = -1;

    private File debugfiletrigger;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        Config conf = new Config();
        conf.putAll(stormConf);

        CallbackCollector callbackCollector = new CallbackCollector(collector);
        this.fetcher = new Fetcher(conf, callbackCollector, allowRedirs(), context);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.ENGLISH);
        long start = System.currentTimeMillis();
        LOG.info("[Fetcher #{}] : starting at {}", taskID, sdf.format(start));

        this.taskID = context.getThisTaskId();

        /**
         * If set to a valid path e.g. /tmp/fetcher-dump-{port} on a worker
         * node, the content of the queues will be dumped to the logs for
         * debugging. The port number needs to match the one used by the
         * FetcherBolt instance.
         **/
        String debugfiletriggerpattern = ConfUtils.getString(conf,
                "fetcherbolt.queue.debug.filepath");

        if (StringUtils.isNotBlank(debugfiletriggerpattern)) {
            debugfiletrigger = new File(
                    debugfiletriggerpattern.replaceAll("\\{port\\}",
                            Integer.toString(context.getThisWorkerPort())));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    @Override
    public void cleanup() {
        this.fetcher.protocolFactory.cleanup();
    }

    @Override
    public void execute(Tuple input) {
    	String urlString = input.getStringByField("url");
    	Metadata metadata = (Metadata) input.getValueByField("metadata");
    	this.fetcher.execute(new TupleWrapper(input), urlString, metadata);
    }


}
