import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, BatchWriteCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'DELETE,OPTIONS',
};

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return ok(null, 200, CORS);
  }

  const params = JSON.parse(event.body);
  console.log(params);

  const userId = event.requestContext?.authorizer?.claims?.sub;
  if (!userId) return err(401, 'Unauthorized', CORS);

  try {
    // Query all items for this user
    const queryParams = {
      TableName: "stock-user-data",
      KeyConditionExpression: "userId = :userId",
      ExpressionAttributeValues: {
        ":userId": userId
      }
    };

    const queryResult = await dynamo.send(new QueryCommand(queryParams));

    // Filter out items that start with "watchlist#"
    const itemsToDelete = queryResult.Items.filter(item =>
      !item.type.startsWith('watchlist#')
    );

    if (itemsToDelete.length === 0) {
      return ok({ message: 'No portfolio items to delete', deletedCount: 0 }, 200, CORS);
    }

    // DynamoDB BatchWrite can handle max 25 items at a time
    const batchSize = 25;
    let deletedCount = 0;

    for (let i = 0; i < itemsToDelete.length; i += batchSize) {
      const batch = itemsToDelete.slice(i, i + batchSize);

      const deleteRequests = batch.map(item => ({
        DeleteRequest: {
          Key: {
            userId: item.userId,
            type: item.type
          }
        }
      }));

      await dynamo.send(new BatchWriteCommand({
        RequestItems: { "stock-user-data": deleteRequests }
      }));
      deletedCount += batch.length;
    }

    return ok({ message: 'Portfolio reset successfully', deletedCount, userId: userId }, 200, CORS);

  } catch (error) {
    console.error('Portfolio reset error:', error);
    return err(500, error.message || 'Failed to reset portfolio', CORS);
  }
};