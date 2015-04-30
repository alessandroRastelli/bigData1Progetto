package org.yottabase.billing.es3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductPair implements WritableComparable<ProductPair> {

	private Text firstProduct;

	private Text secondProduct;

	public ProductPair() {
		super();
	}

	public ProductPair(Text firstProduct, Text secondProduct) {
		super();
		this.changePair(firstProduct, secondProduct);
	}

	public Text getFirstProduct() {
		return firstProduct;
	}

	public Text getSecondProduct() {
		return secondProduct;
	}

	public void changePair(Text firstProduct, Text secondProduct) {
		if (firstProduct.compareTo(secondProduct) <= 0) {
			this.firstProduct = firstProduct;
			this.secondProduct = secondProduct;
		} else {
			this.firstProduct = secondProduct;
			this.secondProduct = firstProduct;
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.firstProduct = new Text(in.readUTF());
		this.secondProduct = new Text(in.readUTF());

	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstProduct.toString());
		out.writeUTF(secondProduct.toString());
	}

	public int compareTo(ProductPair o) {

		int r = this.firstProduct.compareTo(o.getFirstProduct());

		if (r != 0) {
			return r;
		} else {
			return this.secondProduct.compareTo(o.getSecondProduct());
		}

	}

	@Override
	public boolean equals(Object obj) {
		ProductPair that = (ProductPair) obj;

		return (this.firstProduct.equals(that.firstProduct) && this.secondProduct
				.equals(that.secondProduct));
	};

	@Override
	public String toString() {
		return this.firstProduct + "," + this.secondProduct;
	}

}
