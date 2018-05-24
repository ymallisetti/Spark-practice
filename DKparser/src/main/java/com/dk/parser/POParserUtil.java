package com.dk.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dk.domain.pojo.Address;
import com.dk.domain.pojo.Customer;
import com.dk.domain.jaxb.Item;
import com.dk.domain.jaxb.PurchaseOrder;

public class POParserUtil {

	public static Map<String, List<List<String>>> normalizePurchaseOrder(PurchaseOrder purchaseOrderJaxb) {
		
		Map<String, List<List<String>>> purchaseOrderMap = new HashMap<String, List<List<String>>>();
		
		com.dk.domain.pojo.PurchaseOrder purchcaseOrder = setAndParsePO(purchaseOrderJaxb);
		
		purchcaseOrder.normalize(purchcaseOrder, purchaseOrderMap);
		
		return purchaseOrderMap;
	}
	
	public static com.dk.domain.pojo.PurchaseOrder setAndParsePO(PurchaseOrder purchaseOrderJaxb){
		com.dk.domain.pojo.PurchaseOrder purchaseOrderPojo = new com.dk.domain.pojo.PurchaseOrder();
		
		//set all values for purchaseOrder (parsing)
		purchaseOrderPojo.setOrderDate(purchaseOrderJaxb.getOrderDate());
		
		//setting customer
		Customer customerPojo = new Customer();
		com.dk.domain.jaxb.Customer customerJaxb = purchaseOrderJaxb.getCustomer();
		Address billingAddressPojo = new Address();
		Address shippingAddressPojo = new Address();
		
		billingAddressPojo.setCity(customerJaxb.getBillingAddress().getCity());
		billingAddressPojo.setCountry(customerJaxb.getBillingAddress().getCountry());
		billingAddressPojo.setPostalCode(customerJaxb.getBillingAddress().getPostalCode());
		billingAddressPojo.setStreet(customerJaxb.getBillingAddress().getStreet());
		
		shippingAddressPojo.setCity(customerJaxb.getShippingAddress().getCity());
		shippingAddressPojo.setCountry(customerJaxb.getShippingAddress().getCountry());
		shippingAddressPojo.setPostalCode(customerJaxb.getShippingAddress().getPostalCode());
		shippingAddressPojo.setStreet(customerJaxb.getShippingAddress().getStreet());
		
		customerPojo.setLoyalty(customerJaxb.getLoyalty());
		customerPojo.setName(customerJaxb.getName());
		customerPojo.setBillingAddress(billingAddressPojo);
		customerPojo.setShippingAddress(shippingAddressPojo);
		
		purchaseOrderPojo.setCustomer(customerPojo);
		
		List<com.dk.domain.pojo.Item> itemsPojoList = new ArrayList<com.dk.domain.pojo.Item>();
		for(Item item:purchaseOrderJaxb.getItems()){
			com.dk.domain.pojo.Item itemPojo = new com.dk.domain.pojo.Item();
			itemPojo.setComment(item.getComment());
			itemPojo.setPrice(item.getPrice());
			itemPojo.setProductName(item.getProductName());
			itemPojo.setQuantity(item.getQuantity());
			itemsPojoList.add(itemPojo);
		}
		purchaseOrderPojo.setItems(itemsPojoList);
		purchaseOrderPojo.setOrderDate(purchaseOrderJaxb.getOrderDate());
		
		
		
		return purchaseOrderPojo;
	}
	
	public enum Loyalty {
	    BRONZE, SILVER, GOLD
	}

}
