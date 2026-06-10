import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const tableName = "stock-app-data";

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT',
};

const getStockData = async (symbol) => {
  const command = new QueryCommand({
    TableName: tableName,
    KeyConditionExpression: "symbol = :symbol",
    ExpressionAttributeValues: {
      ":symbol": symbol
    }
  });

  const response = await dynamo.send(command);
  return response.Items;
};

export const handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return ok(null, 200, CORS);
  }

  const stocks = JSON.parse(event.body).stocks;
  console.log(stocks);

  try {
    const results = await Promise.all(stocks.map((stock) => getStockData(stock)));
    for (const result of results) {
      console.log(result.slice(0, 5));
    }
    return ok(results, 200, CORS);

  } catch (error) {
    console.error(error);
    return err(500, error.message, CORS);
  }
};