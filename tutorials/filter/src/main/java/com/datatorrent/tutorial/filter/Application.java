package com.datatorrent.tutorial.filter;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.lib.filter.FilterOperator;

/**
 * Simple application illustrating file input-output
 */
@ApplicationAnnotation(name="FilterExample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DataGenerator dataGenerator = dag.addOperator("dataGenerator", new DataGenerator());
    FilterOperator filterOperator = dag.addOperator("filterOperator", new FilterOperator());
    
    CsvFormatter selectedFormatter = dag.addOperator("selectedFormatter", new CsvFormatter());
    CsvFormatter rejectedFormatter = dag.addOperator("rejectedFormatter", new CsvFormatter());
    
    StringFileOutputOperator selectedOutput = dag.addOperator("selectedOutput", new StringFileOutputOperator());
    StringFileOutputOperator rejectedOutput = dag.addOperator("rejectedOutput", new StringFileOutputOperator());
    
    dag.addStream("data", dataGenerator.out, filterOperator.input);
    
    dag.addStream("pojoSelected", filterOperator.truePort, selectedFormatter.in);
    dag.addStream("pojoRejected", filterOperator.falsePort, rejectedFormatter.in);
    
    dag.addStream("csvSelected", selectedFormatter.out, selectedOutput.input);
    dag.addStream("csvRejected", rejectedFormatter.out, rejectedOutput.input);
  }
}
