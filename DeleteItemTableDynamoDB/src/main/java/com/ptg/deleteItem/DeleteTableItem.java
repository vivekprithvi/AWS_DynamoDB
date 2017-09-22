package com.ptg.deleteItem;

	import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Region;
	import com.amazonaws.regions.Regions;
	import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
	import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
	import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
	import com.amazonaws.services.lambda.runtime.RequestHandler;

	public class DeleteTableItem 
	implements RequestHandler<PersonRequest, PersonResponse> {
	   
	  private DynamoDB dynamoDb;
	  private String DYNAMODB_TABLE_NAME = "Person";
	  private Regions REGION = Regions.US_WEST_2;
	  private int id=1001;
	  AmazonDynamoDBClient client;

	  public PersonResponse handleRequest(
	    PersonRequest personRequest, Context context) {

	      this.initDynamoDbClient();

	      persistData(personRequest);

	      PersonResponse personResponse = new PersonResponse();
	      personResponse.setMessage("Updated Successfully!!!");
	      return personResponse;
	  }

	  private DeleteItemOutcome persistData(PersonRequest personRequest) 
	    throws ConditionalCheckFailedException {
	     
		  DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
	              .withPrimaryKey("id",personRequest.getId());
	      return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME).deleteItem(deleteItemSpec);
	  }
	  

	  private void initDynamoDbClient() {
	      client = new AmazonDynamoDBClient();
	      client.setRegion(Region.getRegion(REGION));
	      this.dynamoDb = new DynamoDB(client);
	  }
}
