import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);
const tableName = 'stock-app-data';

const getSparklineData = async (symbol) => {
  try {
    // Get oldest item (for percent change baseline)
    const firstItemParams = {
      TableName: tableName,
      KeyConditionExpression: "symbol = :pkvalue",
      ExpressionAttributeValues: {
        ":pkvalue": symbol
      },
      Limit: 1,
      ScanIndexForward: true  // Oldest first
    };

    // Get last 30 items (for sparkline)
    const sparklineParams = {
      TableName: tableName,
      KeyConditionExpression: "symbol = :pkvalue",
      ExpressionAttributeValues: {
        ":pkvalue": symbol
      },
      Limit: 60,
      ScanIndexForward: false  // Newest first
    };

    const [firstResult, sparklineResult] = await Promise.all([
      dynamo.send(new QueryCommand(firstItemParams)),
      dynamo.send(new QueryCommand(sparklineParams))
    ]);

    if (!firstResult.Items?.length) {
      console.warn(`⚠️ No data found for symbol: ${symbol}`);
      return null;
    }

    if (!sparklineResult.Items?.length) {
      console.warn(`⚠️ No sparkline data found for symbol: ${symbol}`);
      return null;
    }

    const first = firstResult.Items[0];  // Oldest item
    const latest = sparklineResult.Items[0];  // Newest item

    // Reverse the array so sparkline displays oldest → newest (left to right)
    const prices = sparklineResult.Items
      .reverse()  // Now oldest → newest
      .map((item) => item.close);

    const percentChange = ((latest.close - first.close) / first.close) * 100;

    console.log(`✅ ${symbol}: ${percentChange.toFixed(2)}%, ${prices.length} data points`);

    return {
      symbol,
      prices,
      price: latest.close,
      percentChange: parseFloat(percentChange.toFixed(2)),  // Round to 2 decimals
    };
  } catch (error) {
    console.error(`❌ Error fetching data for ${symbol}:`, error);
    return null;  // Return null instead of throwing
  }
};

export const handler = async () => {
  const symbols = [
    'SPY', 'DIA', 'QQQ',  // Indices
    'XLK', 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLU', 'XLY', 'XLP', 'XLRE', 'XLC'  // Sectors
  ];

  try {
    // Fetch all symbols in parallel (faster than sequential)
    const results = await Promise.all(
      symbols.map(symbol => getSparklineData(symbol))
    );

    // Filter out null results (failed fetches)
    const data = results.filter(item => item !== null);

    if (data.length === 0) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: 'Failed to fetch any stock data' }),
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Allow-Methods': 'GET,OPTIONS'
        },
      };
    }

    console.log(`✅ Successfully fetched ${data.length}/${symbols.length} symbols`);

    return {
      statusCode: 200,
      body: JSON.stringify(data),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
    };
  } catch (error) {
    console.error('❌ Lambda error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ 
        error: 'Internal server error',
        message: error.message 
      }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
    };
  }
};