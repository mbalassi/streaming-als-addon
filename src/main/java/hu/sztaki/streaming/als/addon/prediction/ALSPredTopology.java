package hu.sztaki.streaming.als.addon.prediction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ALSPredTopology {

	private static final int TOP_ITEM_COUNT = 2;
	private static final int PARTITION_COUNT = 2;

	public static class IDSpout extends BaseRichSpout {
		SpoutOutputCollector _collector;

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void nextTuple() {

			long uid = getNextID();
			_collector.emit(new Values(uid, getUserVector(uid)));

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public long getNextID() {
			// TODO get user IDs from some queue
			Random rnd = new Random();
			return rnd.nextInt(10);
		}

		public Double[] getUserVector(long uid) {
			// TODO get user vector from database
			return new Double[] { 0.1, 0.2, 1. };
		}

		@Override
		public void ack(Object id) {
		}

		@Override
		public void fail(Object id) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("uid", "userVector"));
		}
	}

	public static class partialTopItemsBolt extends BaseBasicBolt {

		// TODO: generate partial item feature matrix from the database
		double[][] partialItemFeature = new double[][] { { 0.1, 0.2, 1 }, { 0.3, 0.4, 1 },
				{ 1., 1., 1 } };
		Long[] itemIDs = new Long[] { 1L, 10L, 11L }; // Global IDs of the Item
														// partition

		Double[] partialTopItemScores = new Double[TOP_ITEM_COUNT];
		Long[] partialTopItemIDs = new Long[TOP_ITEM_COUNT];

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			Double[] userVector = (Double[]) tuple.getValueByField("userVector");
			Double[] scores = new Double[itemIDs.length];

			// calculate scores for all items
			for (int item = 0; item < itemIDs.length; item++) {
				Double score = 0.;
				for (int i = 0; i < partialItemFeature[item].length; i++) {
					score += partialItemFeature[item][i] * userVector[i];
				}
				scores[item] = score;

			}

			// get the top TOP_ITEM_COUNT items for the partitions
			partialTopItemIDs = Arrays.copyOfRange(itemIDs, 0, TOP_ITEM_COUNT);
			partialTopItemScores = Arrays.copyOfRange(scores, 0, TOP_ITEM_COUNT);

			for (int item = TOP_ITEM_COUNT; item < itemIDs.length; item++) {
				// Assuming scores are positive
				for (int i = 0; i < partialTopItemScores.length; i++) {
					if (scores[item] > partialTopItemScores[i]) {
						partialTopItemScores[i] = scores[item];
						partialTopItemIDs[i] = itemIDs[item];
						break;
					}
				}
			}
			collector.emit(new Values(tuple.getLong(0), partialTopItemIDs, partialTopItemScores));

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("uid", "pTopIDs", "pTopScores"));
		}
	}

	public static class topItemsBolt extends BaseBasicBolt {

		Map<Long, Integer> partitionCount = new HashMap<Long, Integer>();
		Map<Long, Long[]> topIDs = new HashMap<Long, Long[]>();
		Map<Long, Double[]> topScores = new HashMap<Long, Double[]>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			Long uid = tuple.getLong(0);
			Long[] pTopIds = (Long[]) tuple.getValue(1);
			Double[] pTopScores = (Double[]) tuple.getValue(2);

			if (partitionCount.containsKey(uid)) {

				updateTopItems(uid, pTopIds, pTopScores);
				Integer newCount = partitionCount.get(uid) - 1;

				if (newCount > 0) {
					partitionCount.put(uid, newCount);
				} else {
					collector.emit(new Values(uid, topIDs.get(uid), topScores.get(uid)));
					partitionCount.remove(uid);
					topIDs.remove(uid);
					topScores.remove(uid);

				}
			} else {

				if (PARTITION_COUNT == 1) {
					collector.emit(new Values(uid, pTopIds, pTopScores));
				} else {
					partitionCount.put(uid, PARTITION_COUNT - 1);
					topIDs.put(uid, pTopIds);
					topScores.put(uid, pTopScores);
				}
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("uid", "topIDs", "topScores"));
		}

		private void updateTopItems(Long uid, Long[] pTopIDs, Double[] pTopScores) {

			Double[] currentTopScores = topScores.get(uid);
			Long[] currentTopIDs = topIDs.get(uid);

			for (int i = 0; i < pTopScores.length; i++) {
				if (!Arrays.asList(currentTopIDs).contains(pTopIDs[i])) {
					for (int j = 0; j < currentTopScores.length; j++) {
						if (pTopScores[i] > currentTopScores[j]) {
							currentTopScores[j] = pTopScores[i];
							currentTopIDs[j] = pTopIDs[i];
							break;
						}
					}
				}
			}

		}
	}

	public static class topItemProcessBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {

			System.out.println("User:" + tuple.getLong(0));
			System.out.println("Top IDs:" + Arrays.toString((Long[]) tuple.getValue(1)));

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {

		}

	}

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("IDSpuot", new IDSpout(), 1);

		builder.setBolt("partialTop", new partialTopItemsBolt(), PARTITION_COUNT).allGrouping(
				"IDSpuot");
		builder.setBolt("topItems", new topItemsBolt(), 1).fieldsGrouping("partialTop",
				new Fields("uid"));
		builder.setBolt("topItemProcess", new topItemProcessBolt(), 1).shuffleGrouping("topItems");

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
