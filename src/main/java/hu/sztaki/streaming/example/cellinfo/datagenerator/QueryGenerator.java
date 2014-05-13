package hu.sztaki.streaming.example.cellinfo.datagenerator;

import java.util.Random;

public class QueryGenerator {
  private Random r_ = new Random();

  public Query get(int lastMillis) {
    return new Query(System.currentTimeMillis(), lastMillis, r_.nextInt(10000));
  }
}
