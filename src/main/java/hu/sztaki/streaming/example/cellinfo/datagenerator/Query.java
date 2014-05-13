package hu.sztaki.streaming.example.cellinfo.datagenerator;

import java.io.Serializable;

public class Query implements Serializable {

	private static final long serialVersionUID = -2965591677211088503L;
	public final long timeStamp_;
	public final int lastMillis_;
	public final int cellId_;

	public Query(long timeStamp, int lastMillis, int cellId) {
		this.timeStamp_ = timeStamp;
		this.lastMillis_ = lastMillis;
		this.cellId_ = cellId;
	}

}
