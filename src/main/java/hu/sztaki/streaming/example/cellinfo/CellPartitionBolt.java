package hu.sztaki.streaming.example.cellinfo;

import hu.sztaki.streaming.example.cellinfo.model.IWorkerEngine;
import hu.sztaki.streaming.example.cellinfo.model.WorkerEngineBin;
import hu.sztaki.streaming.example.cellinfo.model.WorkerEngineExact;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CellPartitionBolt extends BaseBasicBolt {
	
  Map<String, Integer> counts = new HashMap<String, Integer>();
  //TODO: cellNum
  long _currentTime = 0;
  
  IWorkerEngine engine;
  
  public CellPartitionBolt() {
  	this(0, 10000, 5000);
  }
  
  public CellPartitionBolt(int unitLength, int numOfCells, int bufferInterval)
  {
  	if (unitLength == 0) {
  		engine = new WorkerEngineExact(numOfCells, bufferInterval, _currentTime);
  	}
  	else {
  		engine = new WorkerEngineBin(unitLength, numOfCells, bufferInterval, _currentTime);
  	}
  }
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    int cellId=0;
    long timeStamp=0;
    long lastMillis=0;
    
    //If it comes from the cellInfo spout
    if(tuple.size()==2) {
      cellId = tuple.getInteger(0);
      timeStamp = tuple.getLong(1);
      engine.put(cellId, timeStamp);
    }
    
    //If it comes from the cellQuery spout
    if(tuple.size()==3) {
      cellId = tuple.getInteger(2);
      timeStamp = tuple.getLong(0);
      lastMillis= tuple.getLong(1);
      collector.emit(new Values(cellId, timeStamp,lastMillis,engine.get(timeStamp,lastMillis,cellId )));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cellId","timeStamp","lastMillis","nrInCell"));
  }
}
