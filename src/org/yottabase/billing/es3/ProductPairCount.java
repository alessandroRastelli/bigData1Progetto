package org.yottabase.billing.es3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ProductPairCount implements WritableComparable<ProductPairCount> {

	private ProductPair pair;

	private IntWritable count;

	public ProductPairCount() {
		super();
	}

	public ProductPairCount(ProductPair pair, IntWritable count) {
		super();
		this.pair = pair;
		this.count = count;
	}

	public ProductPair getPair() {
		return pair;
	}

	public void setPair(ProductPair pair) {
		this.pair = pair;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
		if (pair == null)
			pair = new ProductPair();
		pair.readFields(in);

		this.count = new IntWritable(Integer.parseInt(in.readUTF()));
	}

	public void write(DataOutput out) throws IOException {
		pair.write(out);
		out.writeUTF(count.toString());

	}

	public int compareTo(ProductPairCount o) {
		int r = this.getCount().get() - o.getCount().get();

		if (r != 0)
			return r;
		else
			return this.pair.compareTo(o.getPair());
	}

	@Override
	public boolean equals(Object obj) {
		ProductPairCount that = (ProductPairCount) obj;

		return (this.pair.equals(that.pair) && this.count == that.count);
	};

	@Override
	public String toString() {
		return pair.toString() + "\t" + count.toString();
	}

}
