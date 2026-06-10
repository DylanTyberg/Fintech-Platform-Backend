import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET,OPTIONS,PUT',
};

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return ok(null, 200, CORS);
  }

  const params = JSON.parse(event.body);
  console.log(params);

  const userId = event.requestContext?.authorizer?.claims?.sub;
  if (!userId) return err(401, 'Unauthorized', CORS);
  const { type, details, ...remaining } = params;

  const sortKey = `${type}#${details}`;

  const item = {
    TableName: "stock-user-data",
    Item: {
      userId: userId,
      type: sortKey,
      ...remaining
    }
  };

  try {
    await dynamo.send(new PutCommand(item));
    return ok(item.Item, 200, CORS);
  } catch (error) {
    return err(500, error.message, CORS);
  }
};