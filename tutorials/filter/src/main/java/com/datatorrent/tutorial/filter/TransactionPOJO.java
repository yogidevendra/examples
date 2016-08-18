package com.datatorrent.tutorial.filter;

public class TransactionPOJO
{

  long trasactionId;
  double amount;
  long accountNumber;

  public static enum TRANSACTION_TYPE
  {
    CREDIT,
    DEBIT
  }
  
  String type;

  public long getTrasactionId()
  {
    return trasactionId;
  }

  public void setTrasactionId(long trasactionId)
  {
    this.trasactionId = trasactionId;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public long getAccountNumber()
  {
    return accountNumber;
  }

  public void setAccountNumber(long accountNumber)
  {
    this.accountNumber = accountNumber;
  }
  
  public String getType()
  {
    return type;
  }

  public void setType(String type)
  {
    this.type = type;
  }

  @Override
  public String toString()
  {
    return "TransactionPOJO [trasactionId=" + trasactionId + ", amount=" + amount + ", accountNumber=" + accountNumber
        + ", type=" + type + "]";
  }
  
  
}
