import https from 'https';
import { DynamoDBClient, BatchWriteItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';

const dynamodb = new DynamoDBClient({});
const tableName = 'stock-app-data-daily';

const fetchPolygonData = (symbol, from, to, apiKey) => {
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/day/${from}/${to}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;

  console.log("from", from)
  console.log("to", to)

  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = '';
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        try {
          const data = JSON.parse(body);
          console.log(`Polygon raw response for ${symbol}:`, JSON.stringify(data));
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

  
  const todaysDate = new Date();
  const formatedTodaysDate = `${todaysDate.getFullYear()}-${String(todaysDate.getMonth() + 1).padStart(2, '0')}-${String(todaysDate.getDate()).padStart(2, '0')}`;

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
    let formattedLastDate;
    if (checkDatabaseResponse.Items.length > 0) {
      const lastItem = checkDatabaseResponse.Items[checkDatabaseResponse.Items.length - 1];
      const lastItemDate = new Date(lastItem.timestamp.S);
      formattedLastDate = `${lastItemDate.getFullYear()}-${String(lastItemDate.getMonth() + 1).padStart(2, '0')}-${String(lastItemDate.getDate()).padStart(2, '0')}`;


      if (lastItemDate.getFullYear() === todaysDate.getFullYear() && lastItemDate.getMonth() === todaysDate.getMonth() && lastItemDate.getDate() === todaysDate.getDate()) {
        return {
          statusCode: 200,
          headers: {
            "Access-Control-Allow-Origin": "*",
          },
          body: JSON.stringify({ message: "Data already exists for today." }),
        };
      }
      

    }
    else {
      const oneYearAgo = new Date();
      oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
      formattedLastDate = `${oneYearAgo.getFullYear()}-${String(oneYearAgo.getMonth() + 1).padStart(2, '0')}-${String(oneYearAgo.getDate()).padStart(2, '0')}`;

    }

      try {
        const data = await fetchPolygonData(symbol, formattedLastDate, formatedTodaysDate, apiKey);

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
          const timestamp = new Date(bar.t)
          const formattedTimestamp = `${timestamp.getFullYear()}-${String(timestamp.getMonth() + 1).padStart(2, "0")}-${String(timestamp.getDate()).padStart(2, "0")}`;


          items.push({
            PutRequest: {
              Item: {
                symbol: { S: symbol },
                timestamp: { S: formattedTimestamp },
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
};