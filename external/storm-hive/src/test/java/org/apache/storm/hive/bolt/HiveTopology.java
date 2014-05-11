/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hive.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public class HiveTopology {
    static final String USER_SPOUT_ID = "user-spout";
    static final String BOLT_ID = "my-hive-bolt";
    static final String TOPOLOGY_NAME = "hive-test-topology";

    public static void main(String[] args) throws Exception {
        String metaStoreURI = args[0];
        String dbName = args[1];
        String tblName = args[2];
        String[] partNames = {"city","state"};
        String[] colNames = {"id","name","phone","street","city","state"};
        Integer txnsPerBatch = 10;
        Integer batchSize = 100;
        Config config = new Config();
        config.setNumWorkers(1);
        UserDataSpout spout = new UserDataSpout();
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveBolt hiveBolt = new HiveBolt(metaStoreURI,dbName,tblName,mapper);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, hiveBolt, 1)
                .shuffleGrouping(USER_SPOUT_ID);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        waitForSeconds(5);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
        System.exit(0);
    }

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static class UserDataSpout extends BaseRichSpout {
        private ConcurrentHashMap<UUID, Values> pending;
        private SpoutOutputCollector collector;
        private String[] sentences = {
                "1,user1,123456,street1,sunnyvale,ca",
                "2,user2,123456,street2,sunnyvale,ca",
                "3,user3,123456,street3,san jose,ca",
                "4,user4,123456,street4,san jose,ca",
        };
        private int index = 0;
        private int count = 0;
        private long total = 0L;

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "name","phone","street"));
        }

        public void open(Map config, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
            this.pending = new ConcurrentHashMap<UUID, Values>();
        }

        public void nextTuple() {
            String[] user = sentences[index].split(",");
            Values values = new Values(Integer.parseInt(user[0]),user[1],user[2],user[3]);
            UUID msgId = UUID.randomUUID();
            this.pending.put(msgId, values);
            this.collector.emit(values, msgId);
            index++;
            if (index >= sentences.length) {
                index = 0;
            }
            count++;
            total++;
            if(count > 1000){
                count = 0;
                System.out.println("Pending count: " + this.pending.size() + ", total: " + this.total);
            }
            Thread.yield();
        }

        public void ack(Object msgId) {
            this.pending.remove(msgId);
        }

        public void fail(Object msgId) {
            System.out.println("**** RESENDING FAILED TUPLE");
            this.collector.emit(this.pending.get(msgId), msgId);
        }
    }
}
