package org.yottabase.billing.optional.es1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductPairCount implements WritableComparable<ProductPairCount> {

	private Text pair;

	private FloatWritable count;

	public ProductPairCount() {
		super();
	}

	public ProductPairCount(Text pair, FloatWritable count) {
		super();
		this.pair = pair;
		this.count = count;
	}

	public Text getPair() {
		return pair;
	}

	public void setPair(Text pair) {
		this.pair = pair;
	}

	public FloatWritable getCount() {
		return count;
	}

	public void setCount(FloatWritable count) {
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
		this.pair = new Text(in.readUTF());
		this.count = new FloatWritable(Float.parseFloat(in.readUTF()));
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(pair.toString());
		out.writeUTF(count.toString());

	}

	public int compareTo(ProductPairCount o) {
		float r = this.getCount().get() - o.getCount().get();
		
		if (r != 0)
			return (r > 0 ) ? (+1) : (-1);
		else
			return this.pair.compareTo(o.getPair());
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new ProductPairCount(pair, count);
	}


	@Override
	public String toString() {
		return pair.toString() + "\t" + count.toString();
	}

}
