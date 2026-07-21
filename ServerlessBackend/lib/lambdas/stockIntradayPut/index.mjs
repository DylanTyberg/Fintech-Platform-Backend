import https from 'https';
import { DynamoDBClient, BatchWriteItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';
import { ok, err } from "/opt/response.mjs";

const dynamodb = new DynamoDBClient({});
const tableName = process.env.MARKET_CACHE_TABLE || 'stock-app-data';

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
};

const fetchPolygonData = (symbol, fromMs, toMs, apiKey) => {
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/minute/${fromMs}/${toMs}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;

  console.log("from", new Date(fromMs).toISOString());
  console.log("to", new Date(toMs).toISOString());

  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = '';
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        try {
          const data = JSON.parse(body);
          if ((data.status !== 'OK' && data.status !== 'DELAYED') || !data.results) {
            reject(new Error(`Invalid data or no data available: ${JSON.stringify(data)}`));
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

  const symbol = event.queryStringParameters?.symbol;
  if (!symbol) {
    return err(400, "Symbol parameter is required", CORS);
  }

  const apiKey = process.env.POLYGON_API_KEY;

  const checkDatabaseParams = {
    TableName: tableName,
    KeyConditionExpression: 'symbol = :symbol',
    ExpressionAttributeValues: {
      ':symbol': { S: symbol },
    },
    ScanIndexForward: false,
    Limit: 1,
  };

  let lastItemTimestampMs = null;

  try {
    const checkDatabaseResponse = await dynamodb.send(new QueryCommand(checkDatabaseParams));
    if (checkDatabaseResponse.Items.length > 0) {
      lastItemTimestampMs = new Date(checkDatabaseResponse.Items[0].timestamp.S).getTime();
    }
  } catch (error) {
    console.log(JSON.stringify({ message: "Error checking database", error: error.message }));
    return err(500, `Error checking database for ${symbol}: ${error.message}`, CORS);
  }

  // `to` is always "now" — never a computed market-close cutoff. Asking
  // Polygon for bars "up to right now" is always a valid range; there's
  // no scenario where that produces from > to.
  //
  // `from` is the last cached bar if we have one (incremental fetch), or
  // a generous 5-day lookback for a symbol that's never been cached
  // before — wide enough to comfortably cover a weekend or a short
  // holiday run without needing to compute which day the market was
  // actually last open (that day-of-week backtracking is exactly what
  // produced the inverted range this replaces).
  const now = Date.now();
  const from = lastItemTimestampMs ?? (now - 5 * 24 * 60 * 60 * 1000);
  const to = now;

  try {
    const data = await fetchPolygonData(symbol, from, to, apiKey);
    const bars = data.results;

    if (!bars || bars.length === 0) {
      console.log(`No new data for symbol ${symbol}`);
      return ok({ message: "No new data available." }, 200, CORS);
    }

    const BATCH_SIZE = 25;
    const items = bars.map((bar) => ({
      PutRequest: {
        Item: {
          symbol: { S: symbol },
          timestamp: { S: new Date(bar.t).toISOString() },
          category: { S: "intraday" },
          open: { N: bar.o.toString() },
          high: { N: bar.h.toString() },
          low: { N: bar.l.toString() },
          close: { N: bar.c.toString() },
          volume: { N: bar.v.toString() },
        }
      }
    }));

    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE);
      await dynamodb.send(new BatchWriteItemCommand({ RequestItems: { [tableName]: batch } }));
    }

    return ok({
      message: `Stored ${items.length} bars for ${symbol}.`,
      latest: {
        symbol,
        timestamp: new Date(bars[bars.length - 1].t).toISOString(),
        category: "intraday",
        open: bars[bars.length - 1].o,
        high: bars[bars.length - 1].h,
        low: bars[bars.length - 1].l,
        close: bars[bars.length - 1].c,
        volume: bars[bars.length - 1].v,
      },
    }, 200, CORS);

  } catch (error) {
    // Was: log and fall through to an unconditional "success" response —
    // exactly what let the from>to bug go unnoticed. Now surfaces as a
    // real error so a failure here isn't silently indistinguishable from
    // "market's just quiet right now."
    console.log(JSON.stringify({ message: `Error fetching or storing data for ${symbol}`, error: error.message }));
    return err(502, `Failed to fetch data for ${symbol}: ${error.message}`, CORS);
  }
};