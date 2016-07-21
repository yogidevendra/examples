package com.datatorrent.tutorial.filter;
/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {
    runTestApplication(new Application(), 2);
  }

  public static void runTestApplication(StreamingApplication application, int seconds) throws IOException, Exception
  {

    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(application.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(application, conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(30 * 1000); // runs for 30 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
