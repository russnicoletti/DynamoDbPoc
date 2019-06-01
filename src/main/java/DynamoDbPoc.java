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
      System.out.println("action must be supplied ('add-current-items', 'add-history-items', or 'query-items'");
      System.exit(0);
    }

    String action = args[0];

    String region = Regions.US_EAST_1.getName();
    System.out.println("DynamoDB region: " + region);

    AmazonDynamoDB dynamodbClient =
        AmazonDynamoDBClientBuilder.standard().withRegion(region).build();

    int timeBucketMonth = 0;
    int startingFileId  = 0;
    int numberOfItems  = 0;
    switch (action) {
      case "add-current-items":
        if (args.length != 4) {
          System.out.println("Usage: add-current-items, starting fileId number, time-bucket month, number of records");
          System.exit(0);
        }
        startingFileId = Integer.valueOf(args[1]);
        timeBucketMonth = Integer.valueOf(args[2]);
        numberOfItems = Integer.valueOf(args[3]);
        addItems(dynamodbClient, "current", startingFileId, timeBucketMonth, numberOfItems);
        break;

      case "add-history-items":
        if (args.length != 4) {
          System.out.println("Usage: add-history-items, starting fileId number, time-bucket month, number of records");
          System.exit(0);
        }
        startingFileId = Integer.valueOf(args[1]);
        timeBucketMonth = Integer.valueOf(args[2]);
        numberOfItems = Integer.valueOf(args[3]);
        addItems(dynamodbClient, "history", startingFileId, timeBucketMonth, numberOfItems);
        break;

      case "query-current-items":
        if (args.length != 4 && args.length != 5) {
          System.out.println("Usage: query-items, time-bucket month, start day, end day, state (optional)");
          System.exit(0);
        }
        timeBucketMonth = Integer.valueOf(args[1]);
        int startDay = Integer.valueOf(args[2]);
        int endDay = Integer.valueOf(args[3]);

        String state = args.length == 5 ? args[4] : null;
        queryCurrentItems(dynamodbClient, timeBucketMonth, startDay, endDay, state);
        break;

        case "query-history-item":
          String fileId = args[1];
          queryItemHistory(dynamodbClient, fileId);
          break;

      default:
        System.out.println("Unknown action: " + action);
    }
  }

  private static void queryCurrentItems(AmazonDynamoDB dynamodbClient, int timeBucketMonth, int fromDay, int toDay, String state) {

    DynamoDB dynamoDB = new DynamoDB(dynamodbClient);
    Table table = dynamoDB.getTable(tableName);
    Index index = table.getIndex(gsiName);

    String timeBucket = String.format("%d%02d", localDateTime.getYear(), timeBucketMonth);
    String gsiFromSearchKey = String.format("%s%02d000000", timeBucket, fromDay);
    String gsiToSearchKey =   String.format("%s%02d999999", timeBucket, toDay);
    String timeBucketString = "mt:mscn:current:" + timeBucket;

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

  private static void queryItemHistory(AmazonDynamoDB dynamodbClient, String fileId) {
    DynamoDB dynamoDB = new DynamoDB(dynamodbClient);
    Table table = dynamoDB.getTable(tableName);

    String pkValue = String.format("mt:mscn:history:%s", fileId);
    QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("pk = :v_pk")
          .withValueMap(new ValueMap()
        .withString(":v_pk", pkValue));
    String queryDisplayString = String.format("Querying using pk = %s", pkValue);
    System.out.println(queryDisplayString);
    ItemCollection<QueryOutcome> items = table.query(querySpec);
    System.out.println("Query returned:");
   Iterator<Item> iterator = items.iterator();
    while (iterator.hasNext()) {
      Item item = iterator.next();
      System.out.println("item: " + item.toJSONPretty());
    }
  }

  private static void addItems(AmazonDynamoDB dynamodbClient, String type, int startingFileId, int timeBucketMonth, int numberOfItems) {
    System.out.println("startingFileId: " + startingFileId + ", timeBucketMonth: " + timeBucketMonth + ", numberOfItems: " + numberOfItems);

    String state = getState("not-set");
    int dayOfTheMonth = 1;
    for (int i = startingFileId; i < startingFileId + numberOfItems; ++i) {
      try {
        putOneItem(dynamodbClient, type, timeBucketMonth, i, state, dayOfTheMonth);
      } catch (Exception e) {
        System.exit(0);
      }
      state = getState(state);
      dayOfTheMonth = getDayOfMonth(dayOfTheMonth);
    }
  }

  private static void putOneItem (AmazonDynamoDB dynamodbClient, String type, int timeBucketMonth,
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
    String pkValue = String.format("mt:mscn:%s:%s", type, fileId);

    String skValueTimeComponent = "unknown";
    String gsiskValue  = null;
    if (type.compareTo("current") == 0) {
      gsiskValue = String.format("%s:%s",timestamp, fileId);
      skValueTimeComponent = timeBucket;
      attributeValueMap.put("gsisk", new AttributeValue(gsiskValue));
      attributeValueMap.put("state", new AttributeValue(state));
    } else if (type.compareTo("history") == 0) {
      skValueTimeComponent = timestamp;
    }
    String skValue = String.format("mt:mscn:%s:%s", type, skValueTimeComponent);
    attributeValueMap.put("pk", new AttributeValue(pkValue));
    attributeValueMap.put("sk", new AttributeValue(skValue));
    attributeValueMap.put("data", new AttributeValue(jsonContent));

    try {
      dynamodbClient.putItem(new PutItemRequest(tableName, attributeValueMap));
    } catch (Exception e) {
      System.out.println("putItem exception: " + e);
      throw e;
    }
    System.out.println(
        "Successfully added item: (pk: " + pkValue + ", sk: " + skValue + ", gsisk: " + gsiskValue + ") to "
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
