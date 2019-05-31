import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.pinpoint.model.Format;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DynamoDbPoc {

  private static String tableName = "dev-mt-ord";
  private static String gsiName = "gsi-1";
  private static LocalDateTime localDateTime = LocalDateTime.now();

  public static void main(String[] args) {

    if (args.length == 0) {
      System.out.println("action must be supplied ('add-items' or 'query-items'");
      System.exit(0);
    }

    String action = args[0];

    String region = Regions.US_EAST_1.getName();
    System.out.println("DynamoDB region: " + region);

    AmazonDynamoDB dynamodbClient =
        AmazonDynamoDBClientBuilder.standard().withRegion(region).build();

    int timeBucketMonth = 0;
    switch (action) {
      case "add-items":
        if (args.length != 4) {
          System.out.println("Usage: add-items, starting fileId number, time-bucket month, number of records");
          System.exit(0);
        }
        int startingFileId = Integer.valueOf(args[1]);
        timeBucketMonth = Integer.valueOf(args[2]);
        int numberOfItems = Integer.valueOf(args[3]);
        addItems(dynamodbClient, startingFileId, timeBucketMonth, numberOfItems);
        break;
      case "query-items":
        if (args.length != 4 && args.length != 5) {
          System.out.println("Usage: query-items, time-bucket month, start day, end day, state (optional)");
          System.exit(0);
        }
        timeBucketMonth = Integer.valueOf(args[1]);
        int startDay = Integer.valueOf(args[2]);
        int endDay = Integer.valueOf(args[3]);

        String state = args.length == 5 ? args[4] : null;
        queryItems(dynamodbClient, timeBucketMonth, startDay, endDay, state);
        break;
      default:
        System.out.println("Unknown action: " + action);
    }
  }

  private static void queryItems(AmazonDynamoDB dynamodbClient, int timeBucketMonth, int from, int to, String state) {

    DynamoDB dynamoDB = new DynamoDB(dynamodbClient);
    Table table = dynamoDB.getTable(tableName);
    Index index = table.getIndex(gsiName);

    String stateString = (state != null) ? state : "";
    String timeBucket = String.format("%d%02d", localDateTime.getYear(), timeBucketMonth);
    String gsiFromSearchKey = String.format("mt:mscn:state:%s%02d000000:%s", timeBucket, from, stateString);
    String gsiToSearchKey =   String.format("mt:mscn:state:%s%02d999999:%s", timeBucket, to, stateString);
    String timeBucketString = "mt:mscn:state:" + timeBucket;

    QuerySpec querySpec;

    if (state != null) {
      HashMap<String, String> nameMap = new HashMap<>();
      nameMap.put("#state", "state");
      querySpec = new QuerySpec().withKeyConditionExpression("sk = :v_sk and gsisk BETWEEN :v_gsiskFrom AND :v_gsiskTo")
          .withProjectionExpression("#state")
          .withFilterExpression("#state = :v_state")
          .withNameMap(nameMap)
          .withValueMap(new ValueMap()
              .withString(":v_state", state)
              .withString(":v_sk", timeBucketString)
              .withString(":v_gsiskFrom", gsiFromSearchKey)
              .withString(":v_gsiskTo", gsiToSearchKey));
    } else {
      querySpec = new QuerySpec().withKeyConditionExpression("sk = :v_sk and gsisk BETWEEN :v_gsiskFrom AND :v_gsiskTo")
          .withValueMap(new ValueMap()
              .withString(":v_sk", timeBucketString)
              .withString(":v_gsiskFrom", gsiFromSearchKey)
              .withString(":v_gsiskTo", gsiToSearchKey));
    }

    String queryDisplayString = String.format("Querying using sk = %s, gsisk between %s and %s, state: %s", timeBucketString, gsiFromSearchKey, gsiToSearchKey, state != null ? state : "ALL");
    System.out.println(queryDisplayString);
    ItemCollection<QueryOutcome> items = index.query(querySpec);
    System.out.println("Query returned:");
   Iterator<Item> iterator = items.iterator();
    while (iterator.hasNext()) {
      Item item = iterator.next();
      System.out.println("item: " + item.toJSONPretty());
    }
  }

  private static void addItems(AmazonDynamoDB dynamodbClient, int startingFileId, int timeBucketMonth, int numberOfItems) {
    System.out.println("startingFileId: " + startingFileId + ", timeBucketMonth: " + timeBucketMonth + ", numberOfItems: " + numberOfItems);

    String state = getState("not-set");
    int dayOfTheMonth = 1;
    for (int i = startingFileId; i < startingFileId + numberOfItems; ++i) {
      try {
        putOneItem(dynamodbClient, timeBucketMonth, i, state, dayOfTheMonth);
      } catch (Exception e) {
        System.exit(0);
      }
      state = getState(state);
      dayOfTheMonth = getDayOfMonth(dayOfTheMonth);
    }
  }

  private static void putOneItem (AmazonDynamoDB dynamodbClient,int timeBucketMonth,
      int fileIdNumber, String state, int dayOfTheMonth){

    String fileId = "fileId-" + fileIdNumber;
    String timestamp = String.format("%d%02d%02d%02d%02d%02d",
        localDateTime.getYear(), timeBucketMonth,
        dayOfTheMonth, localDateTime.getHour(),
        localDateTime.getMinute(), localDateTime.getSecond());
    String timeBucket = String.format("%d%02d", localDateTime.getYear(), timeBucketMonth);
    String jsonContent =
        "{ \"fileId\": " + fileId + ", \"timestamp\": " + timestamp + ", \"state\": " + state + "}";
    Map<String, AttributeValue> attributeValueMap = new HashMap<>();
    String pkValue = "mt:mscn:state:current-" + fileId;
    String skValue = "mt:mscn:state:" + timeBucket;
    String gsiskValue = "mt:mscn:state:" + timestamp + ":" + state + ":" + fileId;
    attributeValueMap.put("pk", new AttributeValue(pkValue));
    attributeValueMap.put("sk", new AttributeValue(skValue));
    attributeValueMap.put("gsisk", new AttributeValue(gsiskValue));
    attributeValueMap.put("state", new AttributeValue(state));
    attributeValueMap.put("data", new AttributeValue(jsonContent));

    try {
      dynamodbClient.putItem(new PutItemRequest(tableName, attributeValueMap));
    } catch (Exception e) {
      System.out.println("putItem exception: " + e);
      throw e;
    }
    System.out.println(
        "Successfully added item: " + pkValue + ", " + skValue + ", " + gsiskValue + ") to "
            + tableName);
  }

  private static String getState (String currentState) {
    switch (currentState) {
      case "not-set":
        return "PENDING";
      case "PENDING":
        return "SUBMITTED";
      case "SUBMITTED":
        return "OK";
      case "OK":
        return "PENDING";
    }
    return "";
  }

  private static int getDayOfMonth(int currentDayOfTheMonth) {
    return ++currentDayOfTheMonth;
  }
}
