package com.datatorrent.tutorial.filter;

import java.util.Random;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.tutorial.filter.TransactionPOJO.TRANSACTION_TYPE;

public class DataGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

  @Min(1)
  private int numTuples = 20;
  private transient int count = 0;
  private int trasactionId = 10000;
  
  private int sleepTime;
  private transient Random random = new Random();

  public final transient DefaultOutputPort<TransactionPOJO> out = new DefaultOutputPort<TransactionPOJO>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    sleepTime = context.getValue(Context.OperatorContext.SPIN_MILLIS);
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    LOG.debug("beginWindow: windowId = {}", windowId);
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      TransactionPOJO transactionPOJO = new TransactionPOJO();
      transactionPOJO.trasactionId = ++trasactionId;
      
      //Setting amount >= 20000 for every one in five transactions
      if(count%5 != 0){
        transactionPOJO.amount = random.nextInt(10000);
      }
      else{
        transactionPOJO.amount = 20000 + random.nextInt(10000);
      }
      
      transactionPOJO.type = TRANSACTION_TYPE.values()[random.nextInt(2)];
      String accountNumber = RandomStringUtils.randomNumeric(9);
      transactionPOJO.setAccountNumber(Long.parseLong(accountNumber));
      out.emit(transactionPOJO);
    } else {
      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

}
