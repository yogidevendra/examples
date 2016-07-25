package com.datatorrent.tutorial.filter;
/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

import java.io.File;
import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.tutorial.finance.DataGeneratorApp;
import com.datatorrent.tutorial.finance.FraudDetectionApp;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  public static class ApplicationTestWatcher extends TestWatcher{
    
    String baseDir;
    
    @Override
    protected void starting(Description description)
    {
      String className = description.getClassName();

      this.baseDir = "target" + Path.SEPARATOR + className + Path.SEPARATOR + description.getMethodName()
          + Path.SEPARATOR;
      super.starting(description);
      
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    protected void finished(Description description)
    {
      
    }
  }
  
  @Rule
  public ApplicationTestWatcher applicationTestWatcher = new ApplicationTestWatcher();
  
  @Test
  public void testDataGeneratorApp() throws IOException, Exception
  {

    Configuration conf = new Configuration(false);
    conf.addResource(DataGeneratorApp.class.getResourceAsStream("/META-INF/properties-DataGeneratorApp.xml"));
    conf.set("dt.application.FilterExample.operator.selectedOutput.prop.filePath", applicationTestWatcher.baseDir);
    conf.set("dt.application.FilterExample.operator.rejectedOutput.prop.filePath", applicationTestWatcher.baseDir);
    runTestApplication(new DataGeneratorApp(), 10, conf);
  }
  

  @Test
  public void testFraudDetectionApp() throws IOException, Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource(FraudDetectionApp.class.getResourceAsStream("/META-INF/properties-FraudDetectionApp.xml"));
    conf.set("dt.application.FilterExample.operator.selectedOutput.prop.filePath", applicationTestWatcher.baseDir);
    conf.set("dt.application.FilterExample.operator.rejectedOutput.prop.filePath", applicationTestWatcher.baseDir);
    runTestApplication(new FraudDetectionApp(), 10, conf);
  }

  public void runTestApplication(StreamingApplication application, int seconds,Configuration conf) throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      lma.prepareDAG(application, conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(seconds * 1000); // runs for 30 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
