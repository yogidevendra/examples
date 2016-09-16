package com.datatorrent.apps;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.BytesFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="Kafka-to-HDFS-Sync")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("KafkaInput", KafkaSinglePortInputOperator.class);
    BytesFileOutputOperator fileOutputOperator = dag.addOperator("FileOutput", BytesFileOutputOperator.class);
    dag.addStream("Kafka to HDFS", kafkaInputOperator.outputPort, fileOutputOperator.input);
  }

}
