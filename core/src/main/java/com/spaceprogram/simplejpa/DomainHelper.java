package com.spaceprogram.simplejpa;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

/**
 * This is a utility class for doing SimpleDB queries.
 *
 * User: normj
 * Date: Aug 31, 2010
 * Time: 5:55:30 PM
 */
public class DomainHelper {

	/**
	 * A utility method for loading up all the items in a domain.
	 * 
	 * @param db
	 * @param domainName
	 * @return
	 * @throws AmazonClientException
	 */
	public static List<Item> listAllItems(AmazonSimpleDB db, String domainName) throws AmazonClientException {
    	SelectResult results = new SelectResult();

    	List<Item> items = new ArrayList<Item>();
    	do {
    		results = db.select(new SelectRequest()
    			.withConsistentRead(true)
				.withSelectExpression("select * from `" + domainName + "`")
				.withNextToken(results.getNextToken()));
    		
    		items.addAll(results.getItems());
    	} while(results.getNextToken() != null);
    	
		return items;
	}

	/**
	 * Simple wrapper around executing a query.
	 * @param db
	 * @param query
	 * @param nextToken
	 * @return
	 * @throws AmazonClientException
	 */
	public static SelectResult selectItems(AmazonSimpleDB db, String query, String nextToken) throws AmazonClientException {

    	SelectResult results = db.select(new SelectRequest()
			.withConsistentRead(true)
			.withSelectExpression(query)	
			.withNextToken(nextToken));    		
    	
		return results;
	}
	
	/**
	 * Runs a query on the passed in domain with the passed in whereClause.  If the nextToken is included for pagination.
	 * 
	 * @param db
	 * @param domainName
	 * @param whereClause
	 * @param nextToken
	 * @return
	 * @throws AmazonClientException
	 */
	public static SelectResult selectItems(AmazonSimpleDB db, String domainName, String whereClause, String nextToken) throws AmazonClientException {

    	String selectExpression = "select * from `" + domainName + "`";
    	if(whereClause != null) {
    		selectExpression += " where " + whereClause;
    	}

    	
    	SelectResult results = db.select(new SelectRequest()
			.withConsistentRead(true)
			.withSelectExpression(selectExpression)		
			.withNextToken(nextToken));    		
    	
		return results;
	}
	
	/**
	 * Gets the Item based on the item name field which is the same as id in SimpleJPA.
	 * If the item doesn't exist then null is returned.
	 * 
	 * @param db
	 * @param domainName
	 * @param itemName
	 * @return
	 * @throws AmazonClientException
	 */
	public static Item findItemById(AmazonSimpleDB db, String domainName, String itemName) throws AmazonClientException {

		GetAttributesResult results = db.getAttributes(new GetAttributesRequest()
			.withConsistentRead(true)
			.withDomainName(domainName)
			.withItemName(itemName));
		
		if(results.getAttributes().size() == 0) {
			return null;
		}
		
		Item item = new Item(itemName, results.getAttributes());
		return item;		
	}
}
