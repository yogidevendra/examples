/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.apps;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;


import com.datatorrent.api.LocalMode;
import com.datatorrent.apps.Application;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;

import static org.junit.Assert.assertTrue;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String TOPIC = "test";
  private static final int zkPort = 12181;
  private static final int  brokerPort = 9092;

  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

//
//  @Test
//  public void testApplication() throws IOException, Exception {
//    try {
//      // create file in monitored HDFS directory
//      createFile();
//
//      // run app asynchronously; terminate after results are checked
//      LocalMode.Controller lc = asyncRun();
//
//      // get messages from Kafka topic and compare with input
//      chkOutput();
//
//      lc.shutdown();
//    } catch (ConstraintViolationException e) {
//      Assert.fail("constraint violations: " + e.getConstraintViolations());
//    }
//  }
//
//  // create a file with content from 'lines'
//  private void createFile() throws IOException {
//    // remove old file and create new one
//    File file = new File(directory, FILE_NAME);
//    FileUtils.deleteQuietly(file);
//    try {
//      String data = StringUtils.join(lines, "\n") + "\n";    // add final newline
//      FileUtils.writeStringToFile(file, data, "UTF-8");
//    } catch (IOException e) {
//      LOG.error("Error: Failed to create file {} in {}", FILE_NAME, directory);
//      e.printStackTrace();
//    }
//    LOG.debug("Created file {} with {} lines in {}",
//              FILE_NAME, lines.length, directory);
//  }
//
//  private LocalMode.Controller asyncRun() throws Exception {
//    Configuration conf = getConfig();
//    LocalMode lma = LocalMode.newInstance();
//    lma.prepareDAG(new Application(), conf);
//    LocalMode.Controller lc = lma.getController();
//    lc.runAsync();
//    return lc;
//  }
//
//  private Configuration getConfig() {
//      Configuration conf = new Configuration(false);
//      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
//      conf.set("dt.operator.recordReader.prop.files", directory);
//      return conf;
//  }

  private void chkOutput() throws Exception {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    List<String> messages = null;

    // wait for messages to appear in kafka
    Thread.sleep(5000);

    String[] expectedLines = 
        FileUtils.readFileToString(new File("src/test/resources/test_events.txt")).split("\\n");
    
    
    try {
      messages = ku.readMessages(TOPIC, expectedLines.length);
    } catch (Exception e) {
      LOG.error("Error: Got exception {}", e);
    }

    System.out.println("messages:"+messages);
    int i = 0;
    for (String msg : messages) {
      assertTrue("Error: message mismatch", msg.equals(expectedLines[i]));
      ++i;
    }
  }
  
  
private String outputDir;
  
  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }
    
    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      try {
        FileUtils.forceDelete(new File(baseDirectory));
      } catch (IOException e) {
        LOG.error("Error: Got exception {}", e.getMessage());
      }
    }

  }
  
  @Rule
  public TestMeta testMeta = new TestMeta();
  
  @Before
  public void setup() throws Exception
  {
    outputDir = testMeta.baseDirectory + File.separator + "output";
  }
  
  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-test.xml"));
      
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(5000);

      // get messages from Kafka topic and compare with input
      chkOutput();

      lc.shutdown();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
