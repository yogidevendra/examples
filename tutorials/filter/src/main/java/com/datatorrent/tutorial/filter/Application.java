package com.datatorrent.tutorial.filter;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
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
    
    CsvFormatter selectedFormatter = dag.addOperator("selectedFormatter", new CsvFormatter());
    CsvFormatter rejectedFormatter = dag.addOperator("rejectedFormatter", new CsvFormatter());
    
    //ConsoleOutputOperator  filteredInConsole = dag.addOperator("filteredInConsole", new ConsoleOutputOperator());
    StringFileOutputOperator selectedOutput = dag.addOperator("selectedOutput", new StringFileOutputOperator());
    //ConsoleOutputOperator filteredOutConsole = dag.addOperator("filteredOutConsole", new ConsoleOutputOperator());
    StringFileOutputOperator rejectedOutput = dag.addOperator("rejectedOutput", new StringFileOutputOperator());
    
    dag.addStream("data", dataGenerator.out, filterOperator.input);
    
    dag.addStream("pojoSelected", filterOperator.truePort, selectedFormatter.in);
    dag.addStream("pojoRejected", filterOperator.falsePort, rejectedFormatter.in);
    
    dag.addStream("csvSelected", selectedFormatter.out, selectedOutput.input);
    dag.addStream("csvRejected", rejectedFormatter.out, rejectedOutput.input);
    
    dag.getMeta(filterOperator).getMeta(filterOperator.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
    dag.getMeta(selectedFormatter).getMeta(selectedFormatter.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
    dag.getMeta(rejectedFormatter).getMeta(rejectedFormatter.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, TransactionPOJO.class);
  }
}
