package com.datatorrent.tutorial.filter;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.filter.FilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * Simple application illustrating file input-output
 */
@ApplicationAnnotation(name="FilterExample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    DataGenerator dataGenerator = dag.addOperator("dataGenerator", new DataGenerator());
    FilterOperator filterOperator = dag.addOperator("filterOperator", new FilterOperator());
    
    ConsoleOutputOperator  filteredInConsole = dag.addOperator("filteredInConsole", new ConsoleOutputOperator());
    ConsoleOutputOperator filteredOutConsole = dag.addOperator("filteredOutConsole", new ConsoleOutputOperator());
    
    dag.addStream("data", dataGenerator.out, filterOperator.input);
    dag.addStream("in", filterOperator.truePort, filteredInConsole.input);
    dag.addStream("out", filterOperator.falsePort, filteredOutConsole.input);
    
    dag.getMeta(filterOperator).getMeta(filterOperator.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
  }
}
