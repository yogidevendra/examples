/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.BytesFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="HDFS-line-copy")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordReaderModule lineReader = dag.addModule("recordReader", FSRecordReaderModule.class);
    BytesFileOutputOperator fileOutput = dag.addOperator("fileOutput", new BytesFileOutputOperator());
    dag.addStream("line", lineReader.records, fileOutput.input);
  }
}
