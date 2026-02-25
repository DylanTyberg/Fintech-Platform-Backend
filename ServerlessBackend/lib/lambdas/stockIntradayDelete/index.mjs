import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand, BatchWriteCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const tableName = "stock-app-data";

export const handler = async () => {
  try {
    // Scan for items
    let items = [];
    let ExclusiveStartKey;
    do {
      const data = await docClient.send(
        new ScanCommand({
          TableName: tableName,
          FilterExpression: "category = :category",
          ExpressionAttributeValues: { ":category": "intraday" },
          ExclusiveStartKey,
        })
      );
      items = items.concat(data.Items);
      ExclusiveStartKey = data.LastEvaluatedKey;
    } while (ExclusiveStartKey);

    console.log(`Found ${items.length} items to delete.`);

    // Batch delete
    while (items.length) {
      const batch = items.splice(0, 25); // DynamoDB max batch size
      const requestItems = {};
      requestItems[tableName] = batch.map((item) => ({
        DeleteRequest: { Key: { symbol: item.symbol, timestamp: item.timestamp } },
      }));

      await docClient.send(new BatchWriteCommand({ RequestItems: requestItems }));
    }

    console.log("Deletion complete.");
  } catch (err) {
    console.error(err);
  }
};