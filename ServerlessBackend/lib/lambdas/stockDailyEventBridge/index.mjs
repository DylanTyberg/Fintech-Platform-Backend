import https from 'https';
import { DynamoDBClient, BatchWriteItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';

const dynamodb = new DynamoDBClient({});
const tableName = 'stock-app-data-daily';

const fetchPolygonData = (symbol, from, to, apiKey) => {
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/day/${from}/${to}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;

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
          if ((data.status !== 'OK' && data.status !== 'DELAYED') || !data.results) {
            resolve({ results: [] }); // Return empty results instead of rejecting
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
  const apiKey = 'TlYNfcSis7sJMHlwCKfwzpg7cuZlubpU';
  
  // Get all S&P 500 symbols from event
  const symbols = event.symbols || ["AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","TSLA","BRK.B","JPM",
"JNJ","V","MA","UNH","XOM","PG","HD","LLY","AVGO","CVX",
"COST","PEP","KO","MRK","ABBV","WMT","DIS","NFLX","ADBE","CRM",
"AMD","INTC","QCOM","TXN","ORCL","CSCO","IBM","GE","BA","CAT",
"MCD","NKE","LOW","SBUX","GS","MS","BAC","C","BLK", "SPY", "QQQ", "DIA", "IWM", "XLB", "XLE", "XLF", "XLV", "XLI", "XLY", "XLP", "XLK", "XLU", "XLRE", "XLC"
    ]
    ;
  
  const todaysDate = new Date();
  const formattedTodaysDate = `${todaysDate.getFullYear()}-${String(todaysDate.getMonth() + 1).padStart(2, '0')}-${String(todaysDate.getDate()).padStart(2, '0')}`;

  console.log(`Processing ${symbols.length} symbols on ${formattedTodaysDate}`);

  const results = {
    success: [],
    failed: [],
    backfilled: [],
    alreadyExists: []
  };

  // Process all stocks - no delays needed with unlimited calls
  const promises = symbols.map(async (symbol) => {
    try {
      // Check existing data
      const checkParams = {
        TableName: tableName,
        KeyConditionExpression: 'symbol = :symbol',
        ExpressionAttributeValues: {
          ':symbol': { S: symbol },
        },
        ScanIndexForward: false,
        Limit: 1,
      };

      const checkResponse = await dynamodb.send(new QueryCommand(checkParams));
      
      let fromDate;
      let isBackfill = false;

      if (checkResponse.Items && checkResponse.Items.length > 0) {
        const lastItem = checkResponse.Items[0];
        const lastItemDate = new Date(lastItem.timestamp.S);
        
        if (
          lastItemDate.getFullYear() === todaysDate.getFullYear() &&
          lastItemDate.getMonth() === todaysDate.getMonth() &&
          lastItemDate.getDate() === todaysDate.getDate()
        ) {
          console.log(`Data already exists for ${symbol}`);
          results.alreadyExists.push(symbol);
          return;
        }
        
        fromDate = `${lastItemDate.getFullYear()}-${String(lastItemDate.getMonth() + 1).padStart(2, '0')}-${String(lastItemDate.getDate()).padStart(2, '0')}`;
      } else {
        const oneYearAgo = new Date();
        oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
        fromDate = `${oneYearAgo.getFullYear()}-${String(oneYearAgo.getMonth() + 1).padStart(2, '0')}-${String(oneYearAgo.getDate()).padStart(2, '0')}`;
        isBackfill = true;
      }

      const data = await fetchPolygonData(symbol, fromDate, formattedTodaysDate, apiKey);
      const bars = data.results;

      if (!bars || bars.length === 0) {
        results.failed.push({ symbol, reason: 'No data' });
        return;
      }

      const items = bars.map(bar => {
        const timestamp = new Date(bar.t);
        const formattedTimestamp = `${timestamp.getFullYear()}-${String(timestamp.getMonth() + 1).padStart(2, '0')}-${String(timestamp.getDate()).padStart(2, '0')}`;

        return {
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
        };
      });

      // Batch write
      const BATCH_SIZE = 25;
      for (let i = 0; i < items.length; i += BATCH_SIZE) {
        const batch = items.slice(i, i + BATCH_SIZE);
        await dynamodb.send(new BatchWriteItemCommand({
          RequestItems: { [tableName]: batch }
        }));
      }

      if (isBackfill) {
        results.backfilled.push({ symbol, daysAdded: bars.length });
      } else {
        results.success.push({ symbol, daysAdded: bars.length });
      }

    } catch (error) {
      console.error(`Error processing ${symbol}:`, error.message);
      results.failed.push({ symbol, error: error.message });
    }
  });

  // Wait for all stocks to process in parallel
  await Promise.all(promises);

  return {
    statusCode: 200,
    body: JSON.stringify({
      message: `Processed ${symbols.length} stocks for ${formattedTodaysDate}`,
      results: results,
    }),
  };
};