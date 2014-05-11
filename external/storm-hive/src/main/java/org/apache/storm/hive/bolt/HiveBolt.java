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
import org.apache.storm.hive.bolt.mapper.HiveMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;

public class HiveBolt extends  BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HiveBolt.class);
    private OutputCollector collector;
    private Integer currentBatchSize;
    private HiveMapper mapper;

    private static final int defaultMaxOpenConnections = 500;
    private static final int defaultTxnsPerBatch = 100;
    private static final int defaultBatchSize = 15000;
    private static final int defaultCallTimeout = 10000;
    private static final int defaultIdleTimeout = 0;
    private static final int defautHeartBeatInterval = 240; // seconds

    protected String databaseName;
    protected String tableName;
    protected String metaStoreURI;
    protected Integer txnsPerBatch;
    protected Integer batchSize;
    protected Integer maxOpenConnections;
    protected Integer idleTimeout;
    protected Boolean autoCreatePartitions;

    private ExecutorService callTimeoutPool;

    private Integer callTimeout;
    private Integer heartBeatInterval;
    Timer heartBeatTimer = new Timer();
    private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);
    HashMap<HiveEndPoint, HiveWriter> allWriters;

    public HiveBolt(String metaStoreURI,String databaseName,String tableName,HiveMapper mapper) {
        this.metaStoreURI = metaStoreURI;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.mapper = mapper;
        this.currentBatchSize = 0;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector)  {
        try {
            this.collector = collector;
            String timeoutName = "hive-bolt-%d";
            callTimeoutPool = Executors.newFixedThreadPool(1,
                                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
            readHiveConf(conf);
            allWriters = new HashMap<HiveEndPoint,HiveWriter>();
            setupHeartBeatTimer();
        } catch(Exception e) {
            LOG.warn("unable to make connection to hive ",e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            List<String> partitionVals = mapper.mapPartitions(tuple);
            HiveEndPoint endPoint = makeEndPoint(partitionVals);
            HiveWriter writer = getOrCreateWriter(endPoint);
            if(timeToSendHeartBeat.compareAndSet(true, false)) {
                enableHeartBeatOnAllWriters();
            }
            writer.write(tuple);
            currentBatchSize++;
            if(currentBatchSize >= batchSize) {
                writer.flush(true);
                currentBatchSize = 0;
            }
            collector.ack(tuple);
        } catch(Exception e) {
            collector.fail(tuple);
            LOG.warn("hive streaming failed. ",e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        for (Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                HiveWriter w = entry.getValue();
                LOG.info("Flushing writer to {}", w);
                w.flush(false);
                LOG.info("Closing writer to {}", w);
                w.close();
            } catch (Exception ex) {
                LOG.warn("Error while closing writer to " + entry.getKey() +
                         ". Exception follows.", ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
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
        idleTimeout = conf.containsKey("idleTimeout") ? (Integer) conf.get("idleTimeout") : defaultIdleTimeout;
        maxOpenConnections = conf.containsKey("maxOpenConnections") ? (Integer)conf.get("maxOpenConnections") : defaultMaxOpenConnections;
        autoCreatePartitions =  conf.containsKey("autoCreatePartitions") ? (Boolean)conf.get("autoCreatePartitions") : false;
    }

    private void setupHeartBeatTimer() {
        if(heartBeatInterval>0) {
            heartBeatTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        timeToSendHeartBeat.set(true);
                        setupHeartBeatTimer();
                    }
                }, heartBeatInterval * 1000);
        }
    }

    private void enableHeartBeatOnAllWriters() {
        for (HiveWriter writer : allWriters.values()) {
            writer.setHeartBeatNeeded();
        }
    }

    private HiveWriter getOrCreateWriter(HiveEndPoint endPoint)
        throws IOException, InterruptedException, ClassNotFoundException, StreamingException {
        try {
            HiveWriter writer = allWriters.get( endPoint );
            if( writer == null ) {
                LOG.info("Creating Writer to Hive end point : " + endPoint);
                writer = new HiveWriter(endPoint, txnsPerBatch, autoCreatePartitions,
                                        callTimeout, callTimeoutPool, mapper);

                if(allWriters.size() > maxOpenConnections){
                    int retired = retireIdleWriters();
                    if(retired==0) {
                        retireEldestWriter();
                    }
                }
                allWriters.put(endPoint, writer);
            }
            return writer;
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
            throw e;
        } catch (StreamingException e) {
            LOG.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
            throw e;
        }

    }

    private HiveEndPoint makeEndPoint(List<String> partitionVals) throws ConnectionError {
        if(partitionVals==null) {
            return new HiveEndPoint(metaStoreURI, databaseName, tableName, null);
        }
        return new HiveEndPoint(metaStoreURI, databaseName, tableName, partitionVals);
    }

    /**
     * Locate writer that has not been used for longest time and retire it
     */
    private void retireEldestWriter() {
        long oldestTimeStamp = System.currentTimeMillis();
        HiveEndPoint eldest = null;
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            if(entry.getValue().getLastUsed() < oldestTimeStamp) {
                eldest = entry.getKey();
                oldestTimeStamp = entry.getValue().getLastUsed();
            }
        }
        try {
            LOG.info("Closing least used Writer to Hive end point : " + eldest);
            allWriters.remove(eldest).close();
        } catch (IOException e) {
            LOG.warn("Failed to close writer for end point: " + eldest, e);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Locate all writers past idle timeout and retire them
     * @return number of writers retired
     */
    private int retireIdleWriters() {
        int count = 0;
        long now = System.currentTimeMillis();
        ArrayList<HiveEndPoint> retirees = new ArrayList<HiveEndPoint>();

        //1) Find retirement candidates
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            if(now - entry.getValue().getLastUsed() > idleTimeout) {
                ++count;
                retirees.add(entry.getKey());
            }
        }
        //2) Retire them
        for(HiveEndPoint ep : retirees) {
            try {
                LOG.info("Closing idle Writer to Hive end point : {}", ep);
                allWriters.remove(ep).close();
            } catch (IOException e) {
                LOG.warn("Failed to close writer for end point: {}. Error: "+ ep, e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted when attempting to close writer for end point: " + ep, e);
                Thread.currentThread().interrupt();
            }
        }
        return count;
    }

}
