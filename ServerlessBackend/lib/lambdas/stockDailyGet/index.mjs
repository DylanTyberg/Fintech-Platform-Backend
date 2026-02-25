import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

export const handler = async (event) => {
  // Handle OPTIONS for CORS preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
    };
  }

  // Parse body to get array of symbols
  const body = JSON.parse(event.body);
  const symbols = body.symbols; // Expecting { "symbols": ["AAPL", "MSFT", "GOOGL"] }

  if (!symbols || !Array.isArray(symbols) || symbols.length === 0) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "symbols array is required" }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
    };
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
        return {
          symbol: symbol,
          data: data.Items,
          success: true
        };
      } catch (error) {
        console.error(`Error fetching ${symbol}:`, error);
        return {
          symbol: symbol,
          error: error.message,
          success: false
        };
      }
    });

    const results = await Promise.all(promises);

    return {
      statusCode: 200,
      body: JSON.stringify({
        results: results,
        total: symbols.length
      }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
    };
  }
};