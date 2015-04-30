package org.yottabase.billing.optional.es1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductCount implements WritableComparable<ProductCount>{
	
	private Text product;
	
	private IntWritable count;

	public ProductCount() {
		super();
	}

	public ProductCount(Text product, IntWritable count) {
		super();
		this.product = product;
		this.count = count;
	}

	public Text getProduct() {
		return product;
	}

	public void setProduct(Text product) {
		this.product = product;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void readFields(DataInput in) throws IOException {
		this.product = new Text(in.readUTF());
		this.count = new IntWritable(Integer.parseInt(in.readUTF()));
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(product.toString());
		out.writeUTF(count.toString());
	}

	public int compareTo(ProductCount o) {
		
		int p = this.product.compareTo(o.getProduct());
		
		if(p != 0 ){
			return p;
		}else{
			return this.count.compareTo(o.getCount());
		}
	}

	@Override
	public String toString() {
		return	product + ":" + count;
	}
}
