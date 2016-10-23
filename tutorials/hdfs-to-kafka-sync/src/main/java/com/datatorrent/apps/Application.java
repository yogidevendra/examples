/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.apps;

import java.util.Map;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name="HDFS-to-Kafka-Sync")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordReaderModule lineReader = dag.addModule("recordReader", FSRecordReaderModule.class);

    KafkaSinglePortOutputOperator<String,byte[]> kafkaOutput = 
        dag.addOperator("kafkaOutput", new KafkaSinglePortOutputOperator<String,byte[]>());

    dag.addStream("data", lineReader.records, kafkaOutput.inputPort);
    
    
    /*
     * To add custom logic to your DAG, add your custom operator here with
     * dag.addOperator api call and connect it in the dag using the dag.addStream
     * api call. 
     * 
     * For example: 
     * 
     * To parse incoming csv lines, transform them and outputting them on kafka.
     * recordReader->CSVParser->Transform->CSVFormatter->KafkaOutput can be achieved as follows
     * 
     * Adding operators: 
     * CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
     *
     * TransformOperator transform = dag.addOperator("Transform", new TransformOperator());
     * Map<String, String> expMap = Maps.newHashMap();
     * expMap.put("name", "{$.name}.toUpperCase()");
     * transform.setExpressionMap(expMap);
     * CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
     * 
     * Use KafkaSinglePortOutputOperator<String,String> instead of 
     *    KafkaSinglePortOutputOperator<String,String> i.e. 
     * 
     * Replace the following line below
     * KafkaSinglePortOutputOperator<String,byte[]> kafkaOutput = 
     *      dag.addOperator("kafkaOutput", new KafkaSinglePortOutputOperator<String,byte[]>());
     * 
     * with these lines
     * KafkaSinglePortOutputOperator<String,String> kafkaOutput = 
     *      dag.addOperator("kafkaOutput", new KafkaSinglePortOutputOperator<String,String>());
     *
     * Connect these operators with approriate streams
     * Replace the following line below
     * dag.addStream("data", lineReader.records, out.inputPort);
     * 
     * with the following two lines
     * dag.addStream("record", lineReader.records, csvParser.in);
     * dag.addStream("pojo", csvParser.out, formatter.in);
     * dag.addStream("string", formatter.out, kafkaOutput.inputPort);
     *
     * In properties.xml, properties-test.xml->dt.operator.kafkaOutput.prop.producerProperties 
     * Replace
     * serializer.class=kafka.serializer.DefaultEncoder
     * with
     * serializer.class=kafka.serializer.StringEncoder
     * 
     * In ApplicationTests.java->dt.operator.kafkaOutput.prop.producerProperties 
     * Replace
     * test_events.txt with test_events_upper.txt
     * 
     */    
    
  }
}
