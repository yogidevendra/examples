package com.datatorrent.tutorial.finance;

import java.util.Random;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class DataGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

  private static final int HIGH_VALUE = 100000;
  
  @Min(1)
  private int numTuples = 20;
  private int accountNoDigits = 4;
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
      transactionPOJO.amount = generateAmountWithBias();
      String accountNumber = RandomStringUtils.randomNumeric(accountNoDigits);
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
  
  public int generateAmountWithBias(){
    int pivot = random.nextInt(1000);
    //5 in thousand transaction have possibility of high value
    if(pivot < 5){
      return (int) (Math.abs(random.nextGaussian()+1) * HIGH_VALUE);
    }
    else{
      return random.nextInt(50000);
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
