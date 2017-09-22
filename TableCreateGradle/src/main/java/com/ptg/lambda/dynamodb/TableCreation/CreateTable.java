package com.ptg.lambda.dynamodb.TableCreation;
import java.util.ArrayList;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class CreateTable 
implements RequestHandler<PersonRequest, PersonResponse> {
   
  private DynamoDB dynamoDb;
  private String DYNAMODB_TABLE_NAME = "Employee";
  private Regions REGION = Regions.US_WEST_2;
  
  AmazonDynamoDBClient client;

  public PersonResponse handleRequest(
    PersonRequest personRequest, Context context) {

      this.initDynamoDbClient();

      persistData(personRequest);

      PersonResponse personResponse = new PersonResponse();
      personResponse.setMessage("Saved Successfully!!!");
      return personResponse;
  }

  private CreateTableResult persistData(PersonRequest personRequest) 
    throws ConditionalCheckFailedException {
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"Id").withAttributeType("N"));

		ArrayList<KeySchemaElement> ks = new ArrayList<KeySchemaElement>();
		ks.add(new KeySchemaElement().withAttributeName("Id").withKeyType(
				KeyType.HASH));

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
				.withReadCapacityUnits(10L).withWriteCapacityUnits(10L);
      return this.client.createTable(new CreateTableRequest().withTableName(DYNAMODB_TABLE_NAME)
    		  												   .withAttributeDefinitions(attributeDefinitions)
    		  												   .withKeySchema(ks)
    		  												   .withProvisionedThroughput(provisionedThroughput));
  }
  

  private void initDynamoDbClient() {
      client = new AmazonDynamoDBClient();
      client.setRegion(Region.getRegion(REGION));
      this.dynamoDb = new DynamoDB(client);
  }
}