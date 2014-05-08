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
package org.apache.storm.hive.bolt.mapper;


import backtype.storm.tuple.Tuple;
import java.util.List;
import java.io.Serializable;

/**
 * Maps a <code>backtype.storm.tuple.Tupe</code> object
 * to a row in an Hive table.
 */
public interface HiveMapper extends Serializable {

    /**
     * Given a tuple, return a byte array of Hive columns to insert.
     *
     * @param tuple
     * @return
     */

    byte[] columns(Tuple tuple);

    /**
     * Given a tuple, return a hive partition values list.
     *
     * @param tuple
     * @return
     */
    List<String> partitions(Tuple tuple);

    String[] getColumnNames();
    String getFieldDelimiter();

}
