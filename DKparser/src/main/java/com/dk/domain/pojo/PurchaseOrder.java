
package com.dk.domain.pojo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.dk.parser.SequenceGenerator;


public class PurchaseOrder implements DKParserNormalizable{
	
	public PurchaseOrder(){
		setId(SequenceGenerator.getNextSequence());
	}
	
	private int id;
	private static String PojoName = "PurchaseOrder";
	
	private Date orderDate;
	
	
    private List<Item> items;
	
    private Customer customer;
    private String comment;
    
    //fields to be used for normalization
    private Integer customerId;

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

	public Date getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(Date orderDate) {
		this.orderDate = orderDate;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getCustomerId() {
		return customerId;
	}

	public void setCustomerId(Integer customerId) {
		this.customerId = customerId;
	}

	public void normalize(PurchaseOrder normalizablePojo,Map<String, List<List<String>>> finalPoMap) {
		
		List<List<String>> poList = new ArrayList<List<String>>();
		List<String> po = new ArrayList<String>();
		//set items
		for(Item item:items){
			item.normalize(normalizablePojo, finalPoMap);
		}
		
		//set customer
		customer.normalize(normalizablePojo, finalPoMap);
		
		po.add(this.getId().toString());
		po.add(this.getOrderDate().toString());
		po.add(this.getCustomer().getId().toString());
		po.add(PojoName);
		
		poList.add(po);
		finalPoMap.put(PojoName, poList);
	}

}

