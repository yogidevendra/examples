package com.datatorrent.tutorial.finance;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * Simple application illustrating file input-output
 */
@ApplicationAnnotation(name="DataGeneratorApp")
public class DataGeneratorApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    DataGenerator dataGenerator = dag.addOperator("dataGenerator", new DataGenerator());
    DataWriter dataWriter = dag.addOperator("dataWriter", new DataWriter());
    
    //ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("data", dataGenerator.out, dataWriter.input);
    //dag.addStream("data", dataGenerator.out, console.input);
  }
}
