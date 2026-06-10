import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET,OPTIONS',
};

export const handler = async () => {
  const params = {
    TableName: "stock-app-data-movers",
  };

  try {
    const data = await dynamo.send(new ScanCommand(params));
    console.log(data.Items);
    return ok(data.Items, 200, CORS);
  } catch (error) {
    return err(500, error.message, CORS);
  }
};