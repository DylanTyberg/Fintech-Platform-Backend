import https from 'https';
import { DynamoDBClient, BatchWriteItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';

const dynamodb = new DynamoDBClient({});
const tableName = 'stock-app-data';

const fetchPolygonData = (symbol, from, to, apiKey) => {
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/minute/${from}/${to}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;

  console.log("from", new Date(from * 1000).toISOString());
  console.log("to", new Date(to * 1000).toISOString());


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

export const handler = async (event) => {
  
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
      },
    };
  }
  
  const symbol = event.queryStringParameters.symbol;
  const apiKey = 'TlYNfcSis7sJMHlwCKfwzpg7cuZlubpU';

  const checkDatabaseParams = {
    TableName: tableName,
    KeyConditionExpression: 'symbol = :symbol',
    ExpressionAttributeValues: {
      ':symbol': {S: symbol},
    },
    ScanIndexForward: false,
    Limit: 1,
  };

  try {
    const checkDatabaseResponse = await dynamodb.send(new QueryCommand(checkDatabaseParams));
    let lastItemTimestamp;
    if (checkDatabaseResponse.Items.length > 0) {
      const lastItem = checkDatabaseResponse.Items[checkDatabaseResponse.Items.length - 1];
      lastItemTimestamp = new Date(lastItem.timestamp.S);
      

    }

      try {
        let from, to;
        if (lastItemTimestamp) {
          from = Math.floor(lastItemTimestamp.getTime() / 1000);


        }
        else {
          if (lastItemTimestamp) {
            from = Math.floor(lastItemTimestamp.getTime() / 1000);
          } else {
            // Go back 24 hours from now
            const twentyFourHoursAgo = Date.now() - (24 * 60 * 60 * 1000);
            from = Math.floor(twentyFourHoursAgo / 1000);
          }

        }
        
        const nowUTC = Date.now();
        const etOffset = -5 * 60 * 60 * 1000; // ET is UTC-5 (or UTC-4 during DST)

        // Better approach: use a library or manual calculation
        // Manual calculation for ET:
        const now = new Date();
        const utcHour = now.getUTCHours();
        const utcMinutes = now.getUTCMinutes();

        // Convert UTC to ET (assuming EST = UTC-5, adjust for DST if needed)
        const etHour = utcHour - 5;

        // Create date for 4:30 PM ET (which is 9:30 PM UTC in EST, 8:30 PM UTC in EDT)
        const marketClose = new Date();
        marketClose.setUTCHours(21, 30, 0, 0); // 4:30 PM ET = 9:30 PM UTC (EST)

        // If we're before 4:30 PM ET today, go to yesterday
        if (now < marketClose) {
          marketClose.setUTCDate(marketClose.getUTCDate() - 1);
        }

        // Handle weekends
        const dayOfWeek = marketClose.getUTCDay();
        if (dayOfWeek === 0) { // Sunday
          marketClose.setUTCDate(marketClose.getUTCDate() - 2);
        } else if (dayOfWeek === 6) { // Saturday
          marketClose.setUTCDate(marketClose.getUTCDate() - 1);
        }

        to = Math.floor(marketClose.getTime() / 1000);
        const data = await fetchPolygonData(symbol, from, to, apiKey);

        const bars = data.results;

        if (!bars || bars.length === 0) {
          console.log(`No new data for symbol ${symbol}`);
          return {
            statusCode: 200,
            headers: {
              "Access-Control-Allow-Origin": "*",
            },
            body: JSON.stringify({ message: "No new data available." }),
          };
        }

        const BATCH_SIZE = 25;
        const items = [];

        for (const bar of bars) {
          const timestamp = new Date(bar.t).toISOString();

          items.push({
            PutRequest: {
              Item: {
                symbol: { S: symbol },
                timestamp: { S: timestamp },
                category: { S: "intraday" },
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
  } catch (error) {
    console.log(JSON.stringify({ message: "Error checking database", error: error.message }));
  }
  return {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify({ message: "Data stored in DynamoDB successfully for all symbols." }),
  };
}