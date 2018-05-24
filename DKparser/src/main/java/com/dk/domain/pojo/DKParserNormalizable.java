package com.dk.domain.pojo;

import java.util.List;
import java.util.Map;

public interface DKParserNormalizable {
	
	public void normalize(PurchaseOrder normalizablePojo,Map<String, List<List<String>>> finalPoMap);

}
