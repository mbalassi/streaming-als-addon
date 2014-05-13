package hu.sztaki.streaming.example.cellinfo.datagenerator;

import java.io.Serializable;

public class Record implements Serializable {

	private static final long serialVersionUID = -1484005323227609461L;

	public final long timeStamp_;
	public final int cellId_;
	public final int phoneNumber_;

	public Record(long timeStamp, int cellId, int phoneNumber) {
		this.timeStamp_ = timeStamp;
		this.cellId_ = cellId;
		this.phoneNumber_ = phoneNumber;
	}
}
