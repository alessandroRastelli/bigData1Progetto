package org.yottabase.billing.es2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CountByMounth implements WritableComparable<CountByMounth> {

	private int mounth;

	private int count;

	public CountByMounth(int mounth, int count) {
		super();
		this.mounth = mounth;
		this.count = count;
	}

	public CountByMounth() {
		super();
	}

	public int getMounth() {
		return mounth;
	}

	public void setMounth(int mounth) {
		this.mounth = mounth;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
		this.mounth =  Integer.parseInt(in.readUTF());
		this.count = Integer.parseInt(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF( Integer.toString(mounth) );
		out.writeUTF( Integer.toString(count) );
	}

	public int compareTo(CountByMounth o) {
		return this.getMounth() - o.getMounth();
	}

	@Override
	public String toString() {
		return (this.mounth + 1) + "/2015:" + this.count;
	}
}
