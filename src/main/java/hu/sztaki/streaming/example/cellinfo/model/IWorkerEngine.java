package hu.sztaki.streaming.example.cellinfo.model;

public interface IWorkerEngine {
	public int get(long timeStamp, long lastMillis, int cellId);
	public void put(int cellId, long timeStamp);
}
