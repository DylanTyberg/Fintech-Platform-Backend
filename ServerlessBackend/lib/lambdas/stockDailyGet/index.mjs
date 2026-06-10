import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST,OPTIONS',
};

export const handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return ok(null, 200, CORS);
  }

  const body = JSON.parse(event.body);
  const symbols = body.symbols; // Expecting { "symbols": ["AAPL", "MSFT", "GOOGL"] }

  if (!symbols || !Array.isArray(symbols) || symbols.length === 0) {
    return err(400, "symbols array is required", CORS);
  }

  try {
    // Query DynamoDB for each symbol in parallel
    const promises = symbols.map(async (symbol) => {
      const params = {
        TableName: "stock-app-data-daily",
        KeyConditionExpression: "symbol = :pkvalue",
        ExpressionAttributeValues: {
          ":pkvalue": symbol,
        },
        ScanIndexForward: true,
      };

      try {
        const data = await dynamo.send(new QueryCommand(params));
        return { symbol, data: data.Items, success: true };
      } catch (error) {
        console.error(`Error fetching ${symbol}:`, error);
        return { symbol, error: error.message, success: false };
      }
    });

    const results = await Promise.all(promises);

    return ok({ results, total: symbols.length }, 200, CORS);

  } catch (error) {
    return err(500, error.message, CORS);
  }
};