package com.dk.domain.pojo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dk.parser.SequenceGenerator;

public class Address implements DKParserNormalizable{
	private static String PojoName = "Address";
	private Integer id;
    private String street;
    private String city;
    private String postalCode;
    private String country;
    
    public Address(){
    	setId(SequenceGenerator.getNextSequence());
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void normalize(PurchaseOrder normalizablePojo, Map<String, List<List<String>>> finalPoMap) {
		List<List<String>> addressList = finalPoMap.get(PojoName);
		List<String> address = new ArrayList<String>();

		address.add(this.getId().toString());
		address.add(this.getCity());
		address.add(this.getCountry());
		address.add(this.getPostalCode());
		address.add(this.getStreet());
		address.add(PojoName);

		if (null == addressList) {
			addressList = new ArrayList<List<String>>();
		} 
		addressList.add(address);
		finalPoMap.put(PojoName, addressList);
		
	}
}
