package com.dk.domain.pojo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dk.parser.SequenceGenerator;

public class Item implements DKParserNormalizable {
	private static String PojoName = "Item";
	private Integer id;
	private String productName;
	private int quantity;
	private BigDecimal price;
	private String comment;

	// fields to be used for normalization
	private int purchaseOrderId;

	public int getPurchaseOrderId() {
		return purchaseOrderId;
	}

	public void setPurchaseOrderId(int purchaseOrderId) {
		this.purchaseOrderId = purchaseOrderId;
	}

	public Item() {
		setId(SequenceGenerator.getNextSequence());
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void normalize(PurchaseOrder normalizablePojo, Map<String, List<List<String>>> finalPoMap) {
		List<List<String>> itemList = finalPoMap.get(PojoName);
		List<String> item = new ArrayList<String>();

		item.add(this.getId().toString());
		item.add(this.getProductName());
		item.add(this.getQuantity() + "");
		item.add(this.getPrice().toString());

		// add billing address
		item.add(normalizablePojo.getId().toString());
		item.add(PojoName);

		if (null == itemList) {
			itemList = new ArrayList<List<String>>();
		} 
		itemList.add(item);
		finalPoMap.put(PojoName, itemList);
		

	}

}
