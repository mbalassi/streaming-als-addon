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

public class TridentQueryBatchSpout implements IBatchSpout{
  private int _batchSize,_numCells;
  private long _currentTime;
  private Random _rand;
  private int _sleepTime;
  private long _lastmillis;
  
  public TridentQueryBatchSpout() {
    this(1,10,500,2000);
  }
  public TridentQueryBatchSpout(int batchsize,int _sleepTime,int numcells,long _lastmillis) {
    this._batchSize=batchsize;
    this._numCells=numcells;
    this._sleepTime=_sleepTime;
    this._lastmillis=_lastmillis;
  }
  public void open(Map conf, TopologyContext context) {
    _rand=new Random();
  }
  public void emitBatch(long batchId, TridentCollector collector) {
    Utils.sleep(_sleepTime);
    for(int i = 0; i < _batchSize; i++) {
      Utils.sleep(1);
      _currentTime=System.currentTimeMillis();
      collector.emit(new Values(_rand.nextInt(_numCells),_currentTime,_lastmillis));
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
    return new Fields("cellid", "timestamp","lastmillis");
  }
  
  
  
}
