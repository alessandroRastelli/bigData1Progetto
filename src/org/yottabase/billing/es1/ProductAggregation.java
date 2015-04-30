package org.yottabase.billing.es1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ProductAggregation implements Comparable<ProductAggregation>{

	private Text productName;
	
	private IntWritable count;
	
	public ProductAggregation(Text productName, IntWritable count) {
		super();
		this.productName = productName;
		this.count = count;
	}

	public Text getProductName() {
		return productName;
	}

	public void setProductName(Text productName) {
		this.productName = productName;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public int compareTo(ProductAggregation o) {
		int r = (-1) * this.getCount().compareTo(o.getCount());
		
		if( r != 0){
			return r;
		}else{
			return this.getProductName().compareTo(o.getProductName());
		}
	}
	
	
}
