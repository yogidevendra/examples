package com.datatorrent.tutorial.finance;

public class TransactionPOJO
{

  long trasactionId;
  long accountNumber;
  int amount;

  public long getTrasactionId()
  {
    return trasactionId;
  }

  public void setTrasactionId(long trasactionId)
  {
    this.trasactionId = trasactionId;
  }

  public int getAmount()
  {
    return amount;
  }

  public void setAmount(int amount)
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

  @Override
  public String toString()
  {
    return trasactionId + "|" + accountNumber + "|" + amount;
  }
  
  @Override
  public int hashCode()
  {
    return (int) accountNumber%64;
  }
}
