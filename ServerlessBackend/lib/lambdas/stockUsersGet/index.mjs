import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET,OPTIONS',
};

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return ok(null, 200, CORS);
  }

  const userId = event.requestContext?.authorizer?.claims?.sub;
  if (!userId) return err(401, 'Unauthorized', CORS);
  console.log(userId);

  if (!userId) {
    return err(400, 'Missing query param: userId', CORS);
  }

  const params = {
    TableName: "stock-user-data",
    KeyConditionExpression: "userId = :userId",
    ExpressionAttributeValues: {
      ":userId": userId
    }
  };

  try {
    const result = await dynamo.send(new QueryCommand(params));
    return ok(result.Items, 200, CORS);
  } catch (error) {
    return err(500, error.message, CORS);
  }
};