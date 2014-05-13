package hu.sztaki.streaming.example.cellinfo;

import java.util.Map;
import java.util.Random;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TridentInfoBatchSpout implements IBatchSpout{
  private int _batchSize,_numCells;
  private long _currentTime;
  private Random _rand;
  private int _sleepTime;
  
  public TridentInfoBatchSpout() {
    this(10,1,100);
  }
  
  public TridentInfoBatchSpout(int batchsize,int _sleepTime,int numcells) {
    this._batchSize=batchsize;
    this._numCells=numcells;
    this._sleepTime=_sleepTime;
  }
  
  
  
  public void open(Map conf, TopologyContext context) {
    _rand=new Random();
  }
  public void emitBatch(long batchId, TridentCollector collector) {
    
    Utils.sleep(_sleepTime);
    for(int i = 0; i < _batchSize; i++) {
      
      _currentTime=System.currentTimeMillis();
      collector.emit(new Values(_rand.nextInt(_numCells),_currentTime));
    }
  }
  
  public void ack(long batchId) {
    // nothing to do here
  }

  
  public void close() {
    // nothing to do here
  }

  public Map getComponentConfiguration() {
    // no particular configuration here
    return new Config();
  }

  
  public Fields getOutputFields() {
    return new Fields("cellid", "time");
  }
  
  
  
}
