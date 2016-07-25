/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.tutorial.finance;

import java.util.Properties;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Generates transaction data appended to the Customer data and pushes it to Kafka
 * Kafka configuration is exposed as properties. See resources/META-INF/properties-DataGeneratorApp.xml for more details
 *
 */
public class DataWriter extends BaseOperator
{


  // Kafka Properties
  private transient KafkaProducer<String, String> producer;
  @NotNull
  private String topicPrefix;
  @Min(1)
  private int numTopics;
  @NotNull
  private String bootstrapServers; // bootstrap.servers

  public final transient DefaultInputPort<TransactionPOJO> input = new DefaultInputPort<TransactionPOJO>()
  {
    @Override
    public void process(TransactionPOJO tuple)
    {
      processTuple(tuple);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
      Properties properties = new Properties();
      properties.put("bootstrap.servers", bootstrapServers);
      properties.put("acks", "all");
      properties.put("retries", "0");
      properties.put("batch.size", "16384");
      properties.put("auto.commit.interval.ms", "1000");
      properties.put("linger.ms", "100");
      properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      properties.put("block.on.buffer.full", "true");

      producer = new KafkaProducer<>(properties);
  }

  protected void processTuple(TransactionPOJO tuple)
  {
    sendRecordToKafka(tuple.toString());
  }

  public void sendRecordToKafka(String record)
  {
    int topicNum = Math.abs(record.hashCode()) % numTopics;
    producer.send(new ProducerRecord<String, String>(topicPrefix + topicNum, record));
  }

  public String getTopicPrefix()
  {
    return topicPrefix;
  }

  public void setTopicPrefix(String topicPrefix)
  {
    this.topicPrefix = topicPrefix;
  }

  public int getNumTopics()
  {
    return numTopics;
  }

  public void setNumTopics(int numTopics)
  {
    this.numTopics = numTopics;
  }

  public String getBootstrapServers()
  {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers)
  {
    this.bootstrapServers = bootstrapServers;
  }

}
