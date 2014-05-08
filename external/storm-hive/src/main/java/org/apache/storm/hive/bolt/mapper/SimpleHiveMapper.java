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


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SimpleHiveMapper implements HiveMapper {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleHiveMapper.class);
    private static final String DEFAULT_FIELD_DELIMITER = ",";
    private Fields columnFields;
    private Fields partitionFields;
    private String[] columnNames;
    private String fieldDelimiter = DEFAULT_FIELD_DELIMITER;

    public SimpleHiveMapper() {
    }

    public SimpleHiveMapper withColumnFields(Fields columnFields) {
        this.columnFields = columnFields;
        List<String> tempColumnNamesList = this.columnFields.toList();
        columnNames = new String[tempColumnNamesList.size()];
        tempColumnNamesList.toArray(columnNames);
        return this;
    }

    public SimpleHiveMapper withPartitionFields(Fields partitionFields) {
        this.partitionFields = partitionFields;
        return this;
    }

    public SimpleHiveMapper withFieldDelimiter(String delimiter){
        this.fieldDelimiter = delimiter;
        return this;
    }

    @Override
    public byte[] columns(Tuple tuple) {
        StringBuilder builder = new StringBuilder();
        if(this.columnFields != null) {
            for(String field: this.columnFields) {
                builder.append(tuple.getValueByField(field));
                builder.append(fieldDelimiter);
            }
        }
        return builder.toString().getBytes();
    }

    @Override
    public List<String> partitions(Tuple tuple) {
        List<String> partitionList = new ArrayList<String>();
        if(this.partitionFields != null) {
            for(String field: this.partitionFields) {
                partitionList.add(tuple.getStringByField(field));
            }
        }
        return partitionList;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }
}
