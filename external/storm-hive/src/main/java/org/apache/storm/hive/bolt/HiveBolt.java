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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hive.hcatalog.streaming.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class HiveBolt extends  BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HiveBolt.class);
    private HiveWriter writer;
    private OutputCollector collector;
    private HiveEndPoint endPoint;
    private Integer currBatchSize;

    private static final int defaultMaxOpenConnections = 500;
    private static final int defaultTxnsPerBatch = 100;
    private static final int defaultBatchSize = 15000;
    private static final int defaultCallTimeout = 10000;
    private static final int defaultIdleTimeout = 0;
    private static final int defautHeartBeatInterval = 240; // seconds

    HashMap<HiveEndPoint, HiveWriter> allWriters;

    private volatile int idleTimeout;
    protected String databaseName;
    protected String tableName;
    protected String metaStoreURI;
    protected Integer txnsPerBatch;
    protected Integer batchSize;
    private Integer maxOpenConnections;
    protected Boolean autoCreatePartitions;
    protected List<String> partitionVals;
    protected String[] columnVals;

    private ExecutorService callTimeoutPool;

    private Integer callTimeout;
    private Integer heartBeatInterval;
    Timer heartBeatTimer = new Timer();
    private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);

    public HiveBolt(String metaStoreURI,String databaseName,String tableName) {
        this.metaStoreURI = metaStoreURI;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.currBatchSize = 0;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector)  {
        try {
            this.collector = collector;
            String timeoutName = "hive-bolt-%d";
            callTimeoutPool = Executors.newFixedThreadPool(1,
                                                           new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
            readHiveConf(conf);
            endPoint = makeEndPoint();
            writer = new HiveWriter(endPoint,txnsPerBatch,autoCreatePartitions,callTimeout,
                                    callTimeoutPool,columnVals);
        } catch(Exception e) {
            LOG.warn("unable to make connection to hive ",e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if(timeToSendHeartBeat.compareAndSet(true, false)) {
            enableHeartBeatOnAllWriters();
        }
        try {
            collector.ack(tuple);
            writer.write(tuple);
        } catch(Exception e) {
            LOG.warn("hive streaming failed. ",e);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        try {
            writer.flush(false);
            writer.close();
        } catch(Exception e) {
            LOG.warn("unable to close hive connection.",e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }

        ExecutorService toShutdown[] = {callTimeoutPool};
        for (ExecutorService execService : toShutdown) {
            execService.shutdown();
            try {
                while (execService.isTerminated() == false) {
                    execService.awaitTermination(
                                                 Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                LOG.warn("shutdown interrupted on " + execService, ex);
            }
        }

        callTimeoutPool = null;
        super.cleanup();
        LOG.info("Hive Bolt stopped");
    }


    private void readHiveConf(Map conf) {
        String partitions = (String)conf.get("hive.partitions");
        if(partitions != null) {
            partitionVals = Arrays.asList(partitions.split(","));
        }
        String columns = (String)conf.get("hive.columns");
        if (columns != null) {
            columnVals = columns.split(",");
        }

        txnsPerBatch = conf.containsKey("hive.txnsPerBatch") ? (Integer)conf.get("hive.txnsPerBatch") : null;
        if (txnsPerBatch == null || txnsPerBatch < 0) {
            LOG.warn("hive.txnsPerBatch must be a positive number. Defaulting to "+defaultTxnsPerBatch);
            txnsPerBatch = defaultTxnsPerBatch;
        }
        batchSize = conf.containsKey("batchSize") ? (Integer)conf.get("batchSize") : null;
        if(batchSize == null || batchSize<0) {
            LOG.warn("batchSize must be  positive number. Defaulting to " + defaultBatchSize);
            batchSize = defaultBatchSize;
        }

        idleTimeout = conf.containsKey("idleTimeout") ? (Integer)conf.get("idleTimeout") : defaultIdleTimeout;

        callTimeout = conf.containsKey("callTimeout") ? (Integer)conf.get("callTimeout") : null;
        if(callTimeout == null || callTimeout<0) {
            LOG.warn("callTimeout must be  positive number. Defaulting to " + defaultCallTimeout);
            callTimeout = defaultCallTimeout;
        }

        heartBeatInterval = conf.containsKey("heartBeatInterval") ? (Integer)conf.get("heartBeatInterval"): null;
        if(heartBeatInterval == null || heartBeatInterval<0) {
            LOG.warn("heartBeatInterval must be  positive number. Defaulting to " + defautHeartBeatInterval);
            heartBeatInterval = defautHeartBeatInterval;
        }
        maxOpenConnections = conf.containsKey("maxOpenConnections") ? (Integer)conf.get("maxOpenConnections") : defaultMaxOpenConnections;
        autoCreatePartitions =  conf.containsKey("autoCreatePartitions") ? (Boolean)conf.get("autoCreatePartitions") : false;
    }

    private void setupHeartBeatTimer() {
        if(heartBeatInterval>0) {
            heartBeatTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        setupHeartBeatTimer();
                    }
                }, heartBeatInterval * 1000);
        }
    }

    private void enableHeartBeatOnAllWriters() {
        writer.setHearbeatNeeded();
    }

    private HiveEndPoint makeEndPoint() throws ConnectionError {
        if(partitionVals==null) {
            return new HiveEndPoint(metaStoreURI, databaseName, tableName, null);
        }
        return new HiveEndPoint(metaStoreURI, databaseName, tableName, partitionVals);
    }

}
