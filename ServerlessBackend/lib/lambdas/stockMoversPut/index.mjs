import https from 'https';
import { DynamoDBClient, BatchWriteItemCommand, ScanCommand, DeleteItemCommand } from '@aws-sdk/client-dynamodb';
import { ok, err } from "/opt/response.mjs";

const dynamodb = new DynamoDBClient({});
const tableName = 'stock-app-data-movers';

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
};

const deleteAllItems = async (tableName) => {
  let lastEvaluatedKey = undefined;

  do {
    const scanParams = {
      TableName: tableName,
      ProjectionExpression: "direction, symbol",
      ExclusiveStartKey: lastEvaluatedKey,
    };

    const items = await dynamodb.send(new ScanCommand(scanParams));

    if (items.Items.length > 0) {
      // DynamoDB BatchWrite can handle max 25 items per batch
      for (let i = 0; i < items.Items.length; i += 25) {
        const batch = items.Items.slice(i, i + 25);
        const batchDeleteParams = {
          RequestItems: {
            [tableName]: batch.map(item => ({
              DeleteRequest: {
                Key: {
                  direction: item.direction,
                  symbol: item.symbol,
                },
              },
            })),
          },
        };
        await dynamodb.send(new BatchWriteItemCommand(batchDeleteParams));
      }
    }

    lastEvaluatedKey = items.LastEvaluatedKey;
  } while (lastEvaluatedKey);
};

const fetchCompanyName = (apiKey, ticker) => {
  const url = `https://api.polygon.io/v3/reference/tickers/${ticker}?apiKey=${apiKey}`;
  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = '';
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        try {
          const data = JSON.parse(body);
          console.log("company name: ", body.slice(0, 1000));
          if (data.status !== 'OK') {
            reject(new Error('Invalid data or no data available'));
          } else {
            resolve(data);
          }
        } catch (error) {
          reject(error);
        }
      });
    }).on('error', (error) => {
      reject(error);
    });
  });
};

const fetchPolygonData = (apiKey, direction) => {
  const url = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/${direction}?apiKey=${apiKey}`;

  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = '';
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        console.log("Polygon response body:", body.slice(0, 1000));
        try {
          const data = JSON.parse(body);
          console.log("Polygon response status:", data.status);
          console.log("Polygon response results:", data.results ? `array of length${data.results.length}` : data.results);
          if ((data.status !== 'OK' && data.status !== 'DELAYED') || !data.tickers || data.tickers.length === 0) {
            reject(new Error('Invalid data or no data available'));
          } else {
            resolve(data);
          }
        } catch (error) {
          reject(error);
        }
      });
    }).on('error', (error) => {
      reject(error);
    });
  });
};

export const handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return ok(null, 200, CORS);
  }

  const apiKey = process.env.POLYGON_API_KEY;

  await deleteAllItems(tableName);

  try {
    const gainers = await fetchPolygonData(apiKey, "gainers");
    const losers = await fetchPolygonData(apiKey, "losers");

    const gainerBars = gainers.tickers;
    const loserBars = losers.tickers;

    if (!gainerBars || gainerBars.length === 0) {
      console.log(`No new data for symbol`);
    }

    const BATCH_SIZE = 25;
    const items = [];

    for (const bar of gainerBars) {
      let data;
      try {
        data = await fetchCompanyName(apiKey, bar.ticker);
      } catch (error) {
        console.log(`Error fetching company name for ${bar.ticker}: ${error.message}`);
        continue; // Skip this iteration and move to the next one
      }

      if (!data.results || data.results.type !== 'CS') {
        console.log(`Skipping non-common stock ticker: ${bar.ticker} (${data.results?.type})`);
        continue; // Skip warrants, ETFs, etc.
      }

      items.push({
        PutRequest: {
          Item: {
            direction: { S: 'gainers' },
            symbol: { S: bar.ticker },
            name: { S: data.results.name },
            price: { N: bar.day.c.toString() },
            change: { N: bar.todaysChange.toString() },
            percentChange: { N: bar.todaysChangePerc.toString() },
          }
        }
      });
    }

    for (const bar of loserBars) {
      let data;
      try {
        data = await fetchCompanyName(apiKey, bar.ticker);
      } catch (error) {
        console.log(`Error fetching company name for ${bar.ticker}: ${error.message}`);
        continue; // Skip this iteration and move to the next one
      }

      if (!data.results || data.results.type !== 'CS') {
        console.log(`Skipping non-common stock ticker: ${bar.ticker} (${data.results?.type})`);
        continue; // Skip warrants, ETFs, etc.
      }

      items.push({
        PutRequest: {
          Item: {
            direction: { S: 'losers' },
            symbol: { S: bar.ticker },
            name: { S: data.results.name },
            price: { N: bar.day.c.toString() },
            change: { N: bar.todaysChange.toString() },
            percentChange: { N: bar.todaysChangePerc.toString() },
          }
        }
      });
    }

    // Batch write in chunks of 25
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE);
      await dynamodb.send(new BatchWriteItemCommand({ RequestItems: { [tableName]: batch } }));
    }

  } catch (error) {
    console.log(JSON.stringify({ message: `Error fetching or storing data`, error: error.message }));
    return err(500, error.message, CORS);
  }

  return ok({ message: "Data stored in DynamoDB successfully for all symbols." }, 200, CORS);
};