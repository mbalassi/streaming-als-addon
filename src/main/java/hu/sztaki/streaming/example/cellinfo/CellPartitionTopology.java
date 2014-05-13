package hu.sztaki.streaming.example.cellinfo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

//import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
//import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
//import org.apache.storm.hdfs.bolt.format.FileNameFormat;
//import org.apache.storm.hdfs.bolt.format.RecordFormat;
//import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
//import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
//import org.apache.storm.hdfs.bolt.HdfsBolt;

public class CellPartitionTopology {

	private static void wrongArgs() {
		System.out.println("------------------");
		System.out.println("Usage: \"{storm jar, scripts/stormrun.sh} <topologyName> <unitLength> "
				+ "<numOfCells> <infoSpoutSleep> <querySpoutSleep> <queryLastMillis> <workersNum>"
				+ "<infoSpoutParallelism> <querySpoutParallelism> <cellBoltParallelism>\"");
		System.out.println("<cluster> should be \"local\" or the cluster name");
		System.out
				.println("If <unitLength> is 0, WorkerEngineExact is used, otherwise it's the size of a bin.");
		System.out.println("------------------");
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 10) {
			wrongArgs();
		} else {
			String topologyName = "cell-info";
			int unitLength = 10;
			int numOfCells = 13;
			int infoSpoutSleep = 10;
			int querySpoutSleep = 10;
			int queryLastMillis = 1000;
			int workerNum = 3;
			int infoSpoutParallelism = 5;
			int querySpoutParallelism = 5;
			int cellBoltParallelism = 12;

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

				if (numOfCells == 0 || queryLastMillis == 0)
					throw new NumberFormatException("numOfCells or queryLastMillis cannot be 0");
			} catch (NumberFormatException e) {
				wrongArgs();
			}

			TopologyBuilder builder = new TopologyBuilder();

			builder.setSpout("spoutInfo", new RandomCellInfoSpout(infoSpoutSleep, numOfCells),
					infoSpoutParallelism);
			builder.setSpout("spoutQuery", new RandomCellQuerySpout(querySpoutSleep, numOfCells,
					queryLastMillis), querySpoutParallelism);
			builder.setBolt("cellInfo", new CellPartitionBolt(unitLength, 10000, 5000),
					cellBoltParallelism).fieldsGrouping("spoutInfo", new Fields("cellId"))
					.fieldsGrouping("spoutQuery", new Fields("cellId"));

			Config conf = new Config();
			conf.setDebug(false);

			if (topologyName.equals("local")) {
				conf.setMaxTaskParallelism(3);

				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("cell-info", conf, builder.createTopology());

				Thread.sleep(5000);

				cluster.shutdown();
			} else {
				conf.setNumWorkers(workerNum);

				StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
			}
		}
	}
}
