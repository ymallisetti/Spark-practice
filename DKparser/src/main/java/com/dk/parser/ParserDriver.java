package com.dk.parser;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.dk.domain.jaxb.PurchaseOrder;

public class ParserDriver 
{
    public static void main( String[] args ) throws JAXBException, FileNotFoundException
    {
    	File file = new File("D:\\dharmik\\xmlParser\\output\\po_1");
    	InputStream fileAsStream = new FileInputStream(file);
    	// Create the JAXB context
        JAXBContext context = JAXBContext.newInstance(PurchaseOrder.class);

        // Create an unmarshaller
        Unmarshaller unmarshaller = context.createUnmarshaller();

        PurchaseOrder purchaseOrder = (PurchaseOrder) unmarshaller.unmarshal(fileAsStream);
		System.out.println(purchaseOrder);
		Map<String, List<List<String>>> purchaseOrderMap = POParserUtil.normalizePurchaseOrder(purchaseOrder);
		
        // Get the PurchaseOrder object from the JAXB element
        System.out.println("Purchase order for: " + purchaseOrderMap);
    }
    
    public static Map<String, List<List<String>>> parsePOXmls(String inputFileName, String fileContent) throws JAXBException{
    	//File file = new File("D:\\dharmik\\xmlParser\\output\\po_1");
        JAXBContext context = JAXBContext.newInstance(PurchaseOrder.class);

        Unmarshaller unmarshaller = context.createUnmarshaller();

        InputStream fileAsStream = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
        PurchaseOrder purchaseOrder = (PurchaseOrder) unmarshaller.unmarshal(fileAsStream);
        Map<String, List<List<String>>> purchaseOrderMap = POParserUtil.normalizePurchaseOrder(purchaseOrder);
        System.out.println(purchaseOrderMap);
        return purchaseOrderMap;
    }
}
