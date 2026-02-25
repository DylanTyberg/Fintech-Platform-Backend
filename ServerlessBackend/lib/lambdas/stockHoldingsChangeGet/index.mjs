import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

export const handler = async (event) => {
  
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
      },
      body: ''
    };
  }
  
  const body = JSON.parse(event.body);
  const holdings = body.holdings;

  // Execute all queries in parallel
  try {
    const promises = holdings.map(async (holding) => {
      const first_params = {
        TableName: "stock-app-data",
        KeyConditionExpression: "symbol = :symbol",
        ExpressionAttributeValues: {
          ":symbol": holding.symbol
        },
        Limit: 1,
        ScanIndexForward: true
      };
      
      const last_params = {
        TableName: "stock-app-data",
        KeyConditionExpression: "symbol = :symbol",
        ExpressionAttributeValues: {
          ":symbol": holding.symbol
        },
        Limit: 1,
        ScanIndexForward: false
      };

      // Run both queries in parallel for each symbol
      const [first_response, last_response] = await Promise.all([
        dynamo.send(new QueryCommand(first_params)),
        dynamo.send(new QueryCommand(last_params))
      ]);

      return {
        symbol: holding.symbol,
        firstPrice: first_response.Items?.[0],
        lastPrice: last_response.Items?.[0],
        change: first_response.Items?.[0]?.close && last_response.Items?.[0]?.close
        ? (((last_response.Items[0].close - first_response.Items[0].close) / first_response.Items[0].close) * 100).toFixed(2)
        : null
      };
    });

    // Wait for all symbols to complete
    const results = await Promise.all(promises);
    console.log(results);

    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,POST'
      },
      body: JSON.stringify(results)
    };
  } catch (error) {
    console.error("Error fetching stock data:", error);
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,POST'
      },
      body: JSON.stringify({ error: "error fetching data" })
    };
  }
};
