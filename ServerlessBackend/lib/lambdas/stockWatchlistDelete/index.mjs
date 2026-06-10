import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand } from "@aws-sdk/lib-dynamodb";
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
  const { type, symbol } = params;

  // Construct the sort key in the same format as your PUT lambda
  const sortKey = `${type}#${symbol}`;

  const deleteParams = {
    TableName: "stock-user-data",
    Key: {
      userId: userId,
      type: sortKey
    }
  };

  try {
    await dynamo.send(new DeleteCommand(deleteParams));
    return ok({ message: 'Item deleted successfully', userId: user, type: sortKey }, 200, CORS);
  } catch (error) {
    console.error('Delete error:', error);
    return err(500, error.message || 'Failed to delete item', CORS);
  }
};