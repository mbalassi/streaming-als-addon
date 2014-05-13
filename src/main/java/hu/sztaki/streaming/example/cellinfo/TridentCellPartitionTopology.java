package hu.sztaki.streaming.example.cellinfo;



import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TridentCellPartitionTopology {


public static class getTimes extends BaseFunction {
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    long time = tuple.getLong(0);
    long window = tuple.getLong(1);
    for(int i=0;i<window;i++) {
      collector.emit(new Values(time-i));
    }
    }
  }
public static class printOut extends BaseFunction {
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    long value = tuple.getLong(0);
    System.out.println(value);
    collector.emit(new Values(1));
    }
    
  }

  
  private static void wrongArgs() {
    System.out.println("------------------");
    System.out.println("Usage: \"{storm jar, scripts/stormrun.sh} <cluster> <unitLength>"
        + "<numOfCells> <infoSpoutSleep> <querySpoutSleep> <queryLastMillis> <workersNum>"
        + "<infoSpoutParallelism> <querySpoutParallelism> <cellBoltParallelism> <infoSpoutBatch> <querySpoutBatch>\"");
    System.out.println("<cluster> should be \"local\" or the cluster name");
    System.out.println("If <unitLength> is 0, WorkerEngineExact is used, otherwise it's the size of a bin.");
    System.out.println("------------------");
  }

  public static void main(String[] args) throws Exception {
    
    
    if (args == null || args.length != 12) {
      wrongArgs();
      System.out.println(args.length);
    }
    else {
      String topologyName = "cell-info";
      int unitLength = 10;
      int numOfCells = 10;
      int infoSpoutSleep = 0;
      int querySpoutSleep = 100;
      int queryLastMillis = 1000;
      int workerNum = 1;
      int infoSpoutParallelism = 1;
      int querySpoutParallelism = 1;
      int cellBoltParallelism = 1;
      int infoSpoutBatch=100;
      int querySpoutBatch=1;
      
      
      try {
        topologyName = args[0];
        unitLength = Integer.parseInt(args[1]);
        numOfCells = Integer.parseInt(args[2]);
        infoSpoutSleep = Integer.parseInt(args[3]);
        querySpoutSleep = Integer.parseInt(args[4]);
        queryLastMillis = Integer.parseInt(args[5]);
        workerNum = Integer.parseInt(args[6]);
        infoSpoutParallelism = Integer.parseInt(args[7]);
        querySpoutParallelism = Integer.parseInt(args[8]);
        cellBoltParallelism = Integer.parseInt(args[9]);
        infoSpoutBatch= Integer.parseInt(args[10]);
        querySpoutBatch=Integer.parseInt(args[11]);
        
        
        if (numOfCells == 0 || queryLastMillis == 0)
          throw new NumberFormatException("numOfCells or queryLastMillis cannot be 0");
      }
      catch (NumberFormatException e) {
        wrongArgs();
      }
   
    
      TridentTopology topology = new TridentTopology();
      
      TridentState cellCounts = topology.newStream("infoSpout", new TridentInfoBatchSpout(infoSpoutBatch,infoSpoutSleep,numOfCells))
          .parallelismHint(infoSpoutParallelism)
          .groupBy(new Fields("cellid","time"))
          .persistentAggregate(new MemoryMapState.Factory(),new Count(), new Fields("count"))
          .parallelismHint(infoSpoutParallelism);

      topology.newStream("querySpout", new TridentQueryBatchSpout(querySpoutBatch,querySpoutSleep,numOfCells,queryLastMillis))
          .parallelismHint(querySpoutParallelism)
          .each(new Fields("timestamp","lastmillis"), new getTimes(), new Fields("time"))
          .groupBy(new Fields("cellid","time"))
          .stateQuery(cellCounts, new Fields("cellid","time"), new MapGet(), new Fields("count"))
          .each(new Fields("count"),new FilterNull())
          .groupBy(new Fields("cellid","timestamp","lastmillis"))
          .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
          .parallelismHint(querySpoutParallelism);
          //.each(new Fields("sum"), new printOut(),new Fields("print"));
      
      
      Config conf = new Config();
      conf.setMaxSpoutPending(5000);
      
      StormTopology stormtopology= topology.build(); 

      
      
      if (topologyName.equals("local")) {
        conf.setMaxTaskParallelism(8);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TridentCellPartition", conf, stormtopology);
        Utils.sleep(10000);
        cluster.shutdown();
      }
      else {
        conf.setNumWorkers(workerNum);

        StormSubmitter.submitTopology(topologyName, conf, stormtopology);
      }
    }
  }
}
  

