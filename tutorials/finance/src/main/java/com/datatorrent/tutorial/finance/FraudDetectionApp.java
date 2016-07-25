package com.datatorrent.tutorial.finance;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.kafka.KafkaInputOperator;
import com.datatorrent.lib.filter.FilterOperator;

/**
 * Simple application illustrating file input-output
 */
@ApplicationAnnotation(name = "FraudDetectionApp")
public class FraudDetectionApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    // create operators
    KafkaInputOperator kafkaInputOperator = dag.addOperator("kafkaInputOperator", new KafkaInputOperator());
    CsvParser parser = dag.addOperator("parser", new CsvParser());
    FilterOperator filterOperator = dag.addOperator("filterOperator", new FilterOperator());
    CsvFormatter fraudFormatter = dag.addOperator("fraudFormatter", new CsvFormatter());
    CsvFormatter validFormatter = dag.addOperator("validFormatter", new CsvFormatter());
    dag.setAttribute(validFormatter, OperatorContext.PARTITIONER, new StatelessPartitioner<>(2));

    KafkaSinglePortOutputOperator<String, String> fraudTxnKafkaOutput = dag.addOperator("fraudTxnKafkaOutput",
        new KafkaSinglePortOutputOperator<String, String>());
    StringFileOutputOperator validTxnHDFSOutput = dag.addOperator("validTxnHDFSOutput", new StringFileOutputOperator());

    dag.addStream("data", kafkaInputOperator.outputPort, parser.in);
    dag.setInputPortAttribute(parser.in, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("pojo", parser.out, filterOperator.input);
    dag.setInputPortAttribute(filterOperator.input, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("fraudTxn", filterOperator.truePort, fraudFormatter.in);
    dag.setInputPortAttribute(fraudFormatter.in, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("fraudTxnMsg", fraudFormatter.out, fraudTxnKafkaOutput.inputPort);
    dag.setInputPortAttribute(fraudTxnKafkaOutput.inputPort, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("validTxn", filterOperator.falsePort, validFormatter.in);
    dag.setInputPortAttribute(validFormatter.in, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("validTxnMsg", validFormatter.out, validTxnHDFSOutput.input);
    dag.setInputPortAttribute(validTxnHDFSOutput.input, PortContext.PARTITION_PARALLEL, true);
    
    List<String> clusters = new ArrayList<String>();
    clusters.add("node32.morado.com:9098,node34.morado.com:9098,node35.morado.com:9098");
    kafkaInputOperator.setClusters(clusters);
    
    
    List<String> topics = new ArrayList<String>();
    topics.add("transactions0");
    topics.add("transactions1");
    topics.add("transactions2");
    topics.add("transactions3");
    kafkaInputOperator.setTopics(topics);
    
    fraudTxnKafkaOutput.setTopic("fraudTxn");
    
    dag.getMeta(parser).getMeta(parser.out).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
    dag.getMeta(filterOperator).getMeta(filterOperator.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
    dag.getMeta(fraudFormatter).getMeta(fraudFormatter.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
    dag.getMeta(validFormatter).getMeta(validFormatter.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
  }
}
