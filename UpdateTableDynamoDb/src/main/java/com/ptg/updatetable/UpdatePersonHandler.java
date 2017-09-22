package com.ptg.updatetable;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class UpdatePersonHandler 
implements RequestHandler<PersonRequest, PersonResponse> {
   
  private DynamoDB dynamoDb;
  private String DYNAMODB_TABLE_NAME = "Person";
  private Regions REGION = Regions.US_WEST_2;
  private int id=1001;

  public PersonResponse handleRequest(
    PersonRequest personRequest, Context context) {

      this.initDynamoDbClient();

      persistData(personRequest);

      PersonResponse personResponse = new PersonResponse();
      personResponse.setMessage("Updated Successfully!!!");
      return personResponse;
  }

  private UpdateItemOutcome persistData(PersonRequest personRequest) 
    throws ConditionalCheckFailedException {
	  
      UpdateItemSpec updateItemSpec = new UpdateItemSpec()
              .withPrimaryKey("id",id)
              .withUpdateExpression("set firstName=:first, lastName=:last")
              .withValueMap(new ValueMap()
                  .withString(":first", personRequest.getFirstName())
                  .withString(":last", personRequest.getLastName()));
      return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME).updateItem(updateItemSpec);
  }
  

  private void initDynamoDbClient() {
      AmazonDynamoDBClient client = new AmazonDynamoDBClient();
      client.setRegion(Region.getRegion(REGION));
      this.dynamoDb = new DynamoDB(client);
  }
}
