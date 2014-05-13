package hu.sztaki.streaming.example.cellinfo;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomCellInfoSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	Random _rand;
	long _currentTime = 0;
	int _sleepTime;
	int _cellNumber;

	public RandomCellInfoSpout() {
		this(10, 13);
	}

	public RandomCellInfoSpout(int sleepTime, int cellNumber) {
		_sleepTime = sleepTime;
		_cellNumber = cellNumber;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(_sleepTime);
		// currentTime +=_rand.nextInt(sleepTime);
		_currentTime = System.currentTimeMillis();
		_collector.emit(new Values(_rand.nextInt(_cellNumber), _currentTime));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("cellId", "timeStamp"));
	}

}
