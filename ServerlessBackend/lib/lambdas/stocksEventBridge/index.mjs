import https from 'https';
import { DynamoDBClient, BatchWriteItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from "@aws-sdk/util-dynamodb";


const dynamodb = new DynamoDBClient({});
const tableName = 'stock-app-data';

const fetchPolygonData = (symbol, from, to, apiKey) => {
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/minute/${from}/${to}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;

  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = '';
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        try {
          const data = JSON.parse(body);
          //console.log(`Polygon raw response for ${symbol}:`, JSON.stringify(data));
          if ((data.status !== 'OK' && data.status !=='DELAYED') || !data.results) {
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


const getSymbols = async () => {
  const params = {
    TableName: "stock-user-data",
    // Must use ExpressionAttributeValues with DynamoDB types
    FilterExpression: "begins_with(#type, :watchlistPrefix) OR begins_with(#type, :holdingPrefix)",
    ExpressionAttributeNames: {
      "#type": "type"
    },
    ExpressionAttributeValues: {
      ":watchlistPrefix": { S: "watchlist#" },
      ":holdingPrefix": { S: "holding#" }
    }
  };

  try {
    const result = await dynamodb.send(new ScanCommand(params));

    const symbols = new Set();

    for (const rawItem of result.Items ?? []) {
      const item = unmarshall(rawItem);   // convert {S:""} → normal JS
      const symbol = item.type.split("#")[1];
      symbols.add(symbol);
    }
    console.log("Symbols:", symbols)

    return Array.from(symbols);


  } catch (error) {
    console.error("Error getting symbols:", error);
    throw error;
  }
};

export const handler = async (event) => {
  const userSymbols = await getSymbols();
  const indices = ['SPY', 'DIA', 'QQQ'];
  const sectors = ['XLK', 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLU', 'XLY', 'XLP', 'XLRE', 'XLC'];
  
  // Combine all symbols and remove duplicates
  const symbols = [...new Set([...userSymbols, ...indices, ...sectors])];
  const apiKey = 'TlYNfcSis7sJMHlwCKfwzpg7cuZlubpU';

  const easternTime = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/New_York" })
  );

  const day = easternTime.getDay(); // 0 = Sunday, 6 = Saturday
  const hour = easternTime.getHours();
  const minute = easternTime.getMinutes();

  // Skip weekends
  if (day === 0 || day === 6) {
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Market closed (weekend)" }),
    };
  }

  // Skip outside 9:00 AM–4:30 PM Eastern
  if (hour < 9 || (hour === 9 && minute < 15) || (hour === 16 && minute > 45) || hour > 16) {
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Market closed (off hours)" }),
    };
  }

  // Define timestamps (in seconds)
  const currentTimeSec = Math.floor(Date.now() / 1000);
  const to = currentTimeSec - 15 * 60; // 15 minutes ago
  const from = to - 5 * 60;            // 20 to 15 minutes ago


  for (const symbol of symbols) {
    try {
      const data = await fetchPolygonData(symbol, from, to, apiKey);

      const bars = data.results;

      if (!bars || bars.length === 0) {
        console.log(`No new data for symbol ${symbol}`);
        continue;
      }

      const BATCH_SIZE = 25;
      const items = [];

      for (const bar of bars) {
        const timestamp = new Date(bar.t).toISOString(); // Polygon timestamp in ms

        items.push({
          PutRequest: {
            Item: {
              symbol: { S: symbol },
              timestamp: { S: timestamp },
              category: { S: 'intraday' },
              open: { N: bar.o.toString() },
              high: { N: bar.h.toString() },
              low: { N: bar.l.toString() },
              close: { N: bar.c.toString() },
              volume: { N: bar.v.toString() },
            }
          }
        });
      }

      // Batch write in chunks of 25
      for (let i = 0; i < items.length; i += BATCH_SIZE) {
        const batch = items.slice(i, i + BATCH_SIZE);

        const params = {
          RequestItems: {
            [tableName]: batch
          }
        };

        await dynamodb.send(new BatchWriteItemCommand(params));
      }

    } catch (error) {
      console.log(JSON.stringify({ message: `Error fetching or storing data for ${symbol}`, error: error.message }));
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: "Data stored in DynamoDB successfully for all symbols." }),
  };
};