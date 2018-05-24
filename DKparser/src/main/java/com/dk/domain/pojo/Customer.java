package com.dk.domain.pojo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dk.parser.POParserUtil.Loyalty;
import com.dk.parser.SequenceGenerator;

public class Customer implements DKParserNormalizable{
	private static String PojoName = "Customer";
	private Integer id;
    private String name;
    private Address shippingAddress;
    private Address billingAddress;
    private Loyalty loyalty;
    
    //fields to be used for normalization
    private int shippingAddId;
    private int billingAddId;

    public int getShippingAddId() {
		return shippingAddId;
	}

	public void setShippingAddId(int shippingAddId) {
		this.shippingAddId = shippingAddId;
	}

	public int getBillingAddId() {
		return billingAddId;
	}

	public void setBillingAddId(int billingAddId) {
		this.billingAddId = billingAddId;
	}

	public Customer(){
    	setId(SequenceGenerator.getNextSequence());
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Address getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(Address shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public Address getBillingAddress() {
        return billingAddress;
    }

    public void setBillingAddress(Address billingAddress) {
        this.billingAddress = billingAddress;
    }

    public Loyalty getLoyalty() {
        return loyalty;
    }

    public void setLoyalty(Loyalty loyalty) {
        this.loyalty = loyalty;
    }


	public void normalize(PurchaseOrder normalizablePojo,Map<String, List<List<String>>> finalPoMap) {
		List<List<String>> customerList = new ArrayList<List<String>>();
		List<String> customer = new ArrayList<String>();
		
		customer.add(this.getId().toString());
		customer.add(this.getName());
		customer.add(this.getLoyalty().name());
		
		billingAddress.normalize(normalizablePojo, finalPoMap);
		shippingAddress.normalize(normalizablePojo, finalPoMap);
		
		//add billing address
		customer.add(this.getBillingAddress().getId().toString());
		
		//add shipping address
		customer.add(this.getShippingAddress().getId().toString());
		customer.add(PojoName);
		
		customerList.add(customer);
		finalPoMap.put(PojoName, customerList);
		
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
}
