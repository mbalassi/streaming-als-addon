package storm.starter;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class ALSPredTopology {

	public static class IDSpout extends BaseRichSpout {
		SpoutOutputCollector _collector;
		
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void nextTuple() {
			
			long uid = getNextID();
			
			_collector.emit(new Values(uid,getUserVector(uid)));
			
		}

		public long getNextID() {
			
		}

		@Override
		public void ack(Object id) {
		}

		@Override
		public void fail(Object id) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("uid","userVector"));
		}
	}

	

	public static class partialTopItemsBolt extends BaseBasicBolt {

	
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("uid", "pTopIDs","pTopScores"));
		}
	}
	
	public static class topItemsBolt extends BaseBasicBolt {

	
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("uid", "topIDs","topScores"));
		}
	}

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("IDSpuot", new IDSpout(), 1);

		builder.setBolt("partialTop", new partialTopItemsBolt(), 1).allGrouping("IDSpuot");
		builder.setBolt("topItems", new topItemsBolt(), 1).fieldsGrouping("partialTop", new Fields("uid"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ALSPrediction", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();
		}
	}
}
