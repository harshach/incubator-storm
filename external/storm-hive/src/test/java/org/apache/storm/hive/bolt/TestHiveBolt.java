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
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;


import org.apache.hive.hcatalog.streaming.*;

public class TestHiveBolt {
    final static String dbName = "testdb";
    final static String tblName = "test_table1";
    final static String PART1_NAME = "city";
    final static String PART2_NAME = "state";
    final static String[] partNames = { PART1_NAME, PART2_NAME };
    final String partitionVals = "sunnyvale,ca";
    private static final String COL1 = "id";
    private static final String COL2 = "msg";
    final String colNames = "id,msg";
    private String[] colTypes = { "int", "string" };
    private final HiveConf conf;
    private final Driver driver;
    private final int port ;
    final String metaStoreURI;
    private String dbLocation;
    private Config config = new Config();
    private HiveBolt bolt;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Mock
    private IOutputCollector collector;


    private static final Logger LOG = LoggerFactory.getLogger(HiveBolt.class);

    public TestHiveBolt() throws Exception {
        port=9083;
        dbLocation = new String();
        metaStoreURI=null;
        conf = new HiveConf(this.getClass());
        HiveSetupUtil.setConfValues(conf);
        SessionState.start(new CliSessionState(conf));
        driver = new Driver(conf);
        driver.init();
    }

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        HiveSetupUtil.dropDB(conf, dbName);
        dbLocation = "raw://" + dbFolder.newFolder(dbName + ".db").getCanonicalPath();
        HiveSetupUtil.createDbAndTable(conf, dbName, tblName, Arrays.asList(partitionVals.split(",")), colNames.split(","),
                colTypes, partNames, dbLocation);
    }

    // @Test
    // public void testLocalMeatstore() throws Exception {
    //     LOG.info("testing local store");
    //     String metaStoreURI="thrift://127.0.0.1:9083";
    //     bolt = new HiveBolt(metaStoreURI,dbName,tblName);
    //     String newPartitionVals = "san jose,ca";
    //     String colNames1 = "id,name";
    //     config.put("hive.partitions", newPartitionVals);
    //     config.put("hive.columns",colNames1);
    //     config.put("hive.txnsPerBatch",2);
    //     config.put("batchSize",10);
    //     bolt.prepare(config,null,new OutputCollector(collector));
    //     Integer id = 1;
    //     String msg = "test";
    //     for(int i=0; i < 20; i++) {
    //         Tuple tuple = generateTestTuple(id,msg);
    //         bolt.execute(tuple);
    //         verify(collector).ack(tuple);
    //     }
    //     bolt.cleanup();
    // }

    @Test
    public void testEndpointConnection() throws Exception {
        // 1) Basic
        HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName
                                              , Arrays.asList(partitionVals.split(",")));
        StreamingConnection connection = endPt.newConnection(false, null); //shouldn't throw
        connection.close();
        // 2) Leave partition unspecified
        endPt = new HiveEndPoint(metaStoreURI, dbName, tblName, null);
        endPt.newConnection(false, null).close(); // should not throw
    }

    @Test
    public void executeWithByteArrayIdandMessage()
        throws Exception {

        bolt = new HiveBolt(metaStoreURI,dbName,tblName);
        config.put("hive.partitions",partitionVals);
        config.put("hive.columns",colNames);
        config.put("hive.txnsPerBatch",2);
        config.put("batchSize",2);
        config.put("autoCreatePartitions",true);
        bolt.prepare(config,null,new OutputCollector(collector));
        Integer id = 100;
        String msg = "test-123";
        checkRecordCountInTable(tblName,dbName,0);
        for (int i=0; i < 4; i++) {
            Tuple tuple = generateTestTuple(id,msg);
            bolt.execute(tuple);
            verify(collector).ack(tuple);
        }
        bolt.cleanup();
        checkRecordCountInTable(tblName, dbName, 4);
    }

    @Test
    public void executeWithoutPartitions()
        throws Exception {

        bolt = new HiveBolt(metaStoreURI,dbName,tblName);
        config.put("hive.columns",colNames);
        config.put("hive.txnsPerBatch",2);
        config.put("batchSize",2);
        config.put("autoCreatePartitions",false);
        bolt.prepare(config,null,new OutputCollector(collector));
        Integer id = 100;
        String msg = "test-123";
        checkRecordCountInTable(tblName,dbName,0);
        for (int i=0; i < 4; i++) {
            Tuple tuple = generateTestTuple(id,msg);
            bolt.execute(tuple);
            verify(collector).ack(tuple);
        }
        bolt.cleanup();
        checkRecordCountInTable(tblName, dbName, 4);
    }


    @Test
    public void testData()
        throws Exception {
        bolt = new HiveBolt(metaStoreURI,dbName,tblName);
        config.put("hive.partitions", partitionVals);
        config.put("hive.columns",colNames);
        config.put("hive.txnsPerBatch",2);
        config.put("batchSize",1);
        bolt.prepare(config,null,new OutputCollector(collector));
        Tuple tuple1 = generateTestTuple(1,"SJC");
        Tuple tuple2 = generateTestTuple(2,"SFO");
        bolt.execute(tuple1);
        verify(collector).ack(tuple1);
        bolt.execute(tuple2);
        verify(collector).ack(tuple2);
        bolt.cleanup();
        checkDataWritten(tblName, dbName, "1,SJC", "2,SFO");
    }

    @Test
    public void execute100Tuples()
        throws Exception {
        bolt = new HiveBolt(metaStoreURI,dbName,tblName);
        config.put("hive.partitions", partitionVals);
        config.put("hive.columns",colNames);
        config.put("hive.txnsPerBatch",10);
        config.put("batchSize",10);
        bolt.prepare(config,null,new OutputCollector(collector));
        Integer id = 1;
        String msg = "test";
        checkRecordCountInTable(tblName,dbName,0);
        for(int i=0; i < 100; i++) {
            Tuple tuple = generateTestTuple(id,msg);
            bolt.execute(tuple);
            verify(collector).ack(tuple);
        }
        bolt.;
        checkRecordCountInTable(tblName, dbName, 100);
    }

    private void checkRecordCountInTable(String tableName,String dbName,int expectedCount)
        throws CommandNeedRetryException, IOException {
        int count = listRecordsInTable(tableName,dbName).size();
        Assert.assertEquals(expectedCount, count);
    }

    private  ArrayList<String> listRecordsInTable(String tableName,String dbName)
        throws CommandNeedRetryException, IOException {
        driver.compile("select * from " + dbName + "." + tableName);
        ArrayList<String> res = new ArrayList<String>();
        driver.getResults(res);
        return res;
    }

    private void checkDataWritten(String tableName,String dbName,String... row)
        throws CommandNeedRetryException, IOException {
        ArrayList<String> results = listRecordsInTable(tableName,dbName);
        for(int i = 0; i < row.length; i++) {
            String resultRow = results.get(i).split("\t")[0]+","+results.get(i).split("\t")[1];
            assertEquals(row[i],resultRow);
        }
    }

    private Tuple generateTestTuple(Object id, Object msg) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                                                                             new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
                @Override
                public Fields getComponentOutputFields(String componentId, String streamId) {
                    return new Fields("id", "msg");
                }
            };
        return new TupleImpl(topologyContext, new Values(id, msg), 1, "");
    }

}
