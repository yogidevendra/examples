/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.apps;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;

@ApplicationAnnotation(name="HDFS-to-Kafka-Sync")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordReaderModule lineReader = dag.addModule("recordReader", FSRecordReaderModule.class);

    KafkaSinglePortOutputOperator<String,byte[]> out = 
        dag.addOperator("kafkaOutput", new KafkaSinglePortOutputOperator<String,byte[]>());

    dag.addStream("data", lineReader.records, out.inputPort);
  }
}
