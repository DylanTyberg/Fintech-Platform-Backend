import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const lambda = new LambdaClient();
const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const isMarketOpen = () => {
  const easternTime = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/New_York" })
  );
  const day = easternTime.getDay();
  const hour = easternTime.getHours();
  const minute = easternTime.getMinutes();

  // Weekend
  if (day === 0 || day === 6) return false;

  // Before 9:30 AM or after 4:00 PM Eastern
  if (hour < 9 || (hour === 9 && minute < 30) || hour >= 16) return false;

  return true;
};

const getMarketCloseTime = () => {
  const easternTime = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/New_York" })
  );
  easternTime.setHours(16, 0, 0, 0); // 4:00 PM ET
  return easternTime.getTime();
};

export const handler = async (event) => {
  // Handle OPTIONS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
      body: ''
    };
  }

  const symbol = event.queryStringParameters?.symbol;

  if (!symbol) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "Symbol parameter is required" }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      }
    };
  }

  const params = {
    TableName: "stock-app-data",
    KeyConditionExpression: "symbol = :symbol",
    ExpressionAttributeValues: {
      ":symbol": symbol
    },
    ScanIndexForward: false,
    Limit: 1,
  };

  try {
    const data = await dynamo.send(new QueryCommand(params));
    console.log("Initial query:", data.Items);

    const twentyMinutes = 20 * 60 * 1000;
    
    const isStale = data.Items && data.Items.length > 0 
      ? (() => {
          const dataTime = new Date(data.Items[0].timestamp).getTime();
          const currentTime = Date.now();
          
          if (isMarketOpen()) {
            // During market hours: data is stale if older than 20 minutes
            return (currentTime - dataTime) > twentyMinutes;
          } else {
            // After hours: data is stale if it's from before market close
            const marketCloseTime = getMarketCloseTime();
            return dataTime < marketCloseTime;
          }
        })()
      : true;

    if (!data.Items || data.Items.length === 0 || isStale) {
      // Check if we should even try to fetch (don't fetch if market closed for >20 min)
      const currentTime = Date.now();
      const marketCloseTime = getMarketCloseTime();
      
      if (!isMarketOpen() && (currentTime - marketCloseTime) > twentyMinutes) {
        // Market closed for more than 20 minutes
        if (data.Items && data.Items.length > 0) {
          // Return stale data rather than fetching (market is closed)
          return {
            statusCode: 200,
            body: JSON.stringify(data.Items[0]),
            headers: {
              'Access-Control-Allow-Origin': '*',
              'Access-Control-Allow-Headers': 'Content-Type',
              'Access-Control-Allow-Methods': 'GET,OPTIONS'
            },
          };
        } else {
          // No data at all and market is closed
          return {
            statusCode: 404,
            body: JSON.stringify({ error: "No data available - market is closed" }),
            headers: {
              'Access-Control-Allow-Origin': '*',
              'Access-Control-Allow-Headers': 'Content-Type',
              'Access-Control-Allow-Methods': 'GET,OPTIONS'
            }
          };
        }
      }

      // Market is open or recently closed - fetch fresh data
      const payload = {
        queryStringParameters: {
          symbol: symbol
        }
      };

      const lambda_params = {
        FunctionName: process.env.INTRADAY_PUT_FUNCTION_NAME,
        InvocationType: "RequestResponse",
        Payload: JSON.stringify(payload),
      };

      console.log("Invoking Lambda to fetch fresh data...");
      const lambdaResponse = await lambda.send(new InvokeCommand(lambda_params));

      const responsePayload = JSON.parse(Buffer.from(lambdaResponse.Payload).toString());
      console.log("Lambda Response:", JSON.stringify(responsePayload, null, 2));

      // Check for Lambda errors
      if (lambdaResponse.FunctionError) {
        console.error("Lambda execution error:", responsePayload);
        
        // Return stale data if available, otherwise error
        if (data.Items && data.Items.length > 0) {
          return {
            statusCode: 200,
            body: JSON.stringify(data.Items[0]),
            headers: {
              'Access-Control-Allow-Origin': '*',
              'Access-Control-Allow-Headers': 'Content-Type',
              'Access-Control-Allow-Methods': 'GET,OPTIONS'
            },
          };
        }
        
        return {
          statusCode: 500,
          body: JSON.stringify({ error: "Failed to fetch fresh data" }),
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET,OPTIONS'
          }
        };
      }

      // Query again after Lambda execution
      const new_data = await dynamo.send(new QueryCommand(params));
      console.log("New data after Lambda:", new_data.Items);

      if (!new_data.Items || new_data.Items.length === 0) {
        return {
          statusCode: 404,
          body: JSON.stringify({ error: "No data available for symbol" }),
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET,OPTIONS'
          }
        };
      }

      return {
        statusCode: 200,
        body: JSON.stringify(new_data.Items[0]),
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Allow-Methods': 'GET,OPTIONS'
        },
      };
    }

    // Data is fresh, return it
    return {
      statusCode: 200,
      body: JSON.stringify(data.Items[0]),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
    };
  } catch (error) {
    console.error("Error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
    };
  }
};