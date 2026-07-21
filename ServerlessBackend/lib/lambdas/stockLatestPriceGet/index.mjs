import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { ok, err } from "/opt/response.mjs";

const lambda = new LambdaClient();
const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const TABLE = process.env.MARKET_CACHE_TABLE;

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET,OPTIONS',
};

const isMarketOpen = () => {
  const easternTime = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/New_York" })
  );
  const day = easternTime.getDay();
  const hour = easternTime.getHours();
  const minute = easternTime.getMinutes();

  if (day === 0 || day === 6) return false;
  if (hour < 9 || (hour === 9 && minute < 30) || hour >= 16) return false;
  return true;
};

const fetchFreshData = async (symbol) => {
  const payload = { queryStringParameters: { symbol } };
  const lambdaResponse = await lambda.send(new InvokeCommand({
    FunctionName: process.env.INTRADAY_PUT_FUNCTION_NAME,
    InvocationType: "RequestResponse",
    Payload: JSON.stringify(payload),
  }));

  if (lambdaResponse.FunctionError) {
    // Only fires if intraday-put actually threw/crashed — a normal
    // err(...) response does NOT set this, which is why this check alone
    // was never enough to detect a failed fetch.
    const responsePayload = JSON.parse(Buffer.from(lambdaResponse.Payload).toString());
    console.error("Intraday-put crashed:", JSON.stringify(responsePayload));
    return null;
  }

  const responsePayload = JSON.parse(Buffer.from(lambdaResponse.Payload).toString());
  console.log(JSON.stringify({ message: "Intraday-put response", symbol, responsePayload }));

  if (responsePayload.statusCode && responsePayload.statusCode >= 400) {
    console.error(`Intraday-put returned an error for ${symbol}:`, responsePayload.body);
    return null;
  }

  const body = typeof responsePayload.body === 'string'
    ? JSON.parse(responsePayload.body)
    : responsePayload.body;

  // Use what intraday-put actually just fetched and wrote, directly —
  // no second DB read, so nothing to race against eventual consistency.
  return body?.latest ?? null;
};

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return ok(null, 200, CORS);
  }

  const symbol = event.queryStringParameters?.symbol;
  if (!symbol) {
    return err(400, "Symbol parameter is required", CORS);
  }

  const queryParams = {
    TableName: TABLE,
    KeyConditionExpression: "symbol = :symbol",
    ExpressionAttributeValues: { ":symbol": symbol },
    ScanIndexForward: false,
    Limit: 1,
  };

  console.log(JSON.stringify({ message: "Querying for latest price", symbol, table: TABLE }));

  try {
    const data = await dynamo.send(new QueryCommand(queryParams));
    const hasData = data.Items && data.Items.length > 0;
    console.log(JSON.stringify({ message: "Query result", symbol, table: TABLE, hasData, itemCount: data.Items?.length ?? 0 }));

    let needsFreshFetch;
    if (!hasData) {
      // Never cached — always worth trying, market open or not.
      needsFreshFetch = true;
    } else if (isMarketOpen()) {
      const twentyMinutes = 20 * 60 * 1000;
      const dataTime = new Date(data.Items[0].timestamp).getTime();
      needsFreshFetch = (Date.now() - dataTime) > twentyMinutes;
    } else {
      // Market closed, data on file: that data IS the close. Trade
      // against it regardless of how long ago it landed — no more
      // computing "today's close" by hand, which broke on weekends.
      needsFreshFetch = false;
    }

    if (!needsFreshFetch) {
      return ok(data.Items[0], 200, CORS);
    }

    console.log(`Invoking intraday-put for ${symbol}...`);
    const fresh = await fetchFreshData(symbol);

    if (fresh) {
      return ok(fresh, 200, CORS);
    }

    // Invoke succeeded but returned nothing new (or failed) — fall back
    // to cached data if any exists rather than a hard error.
    if (hasData) {
      return ok(data.Items[0], 200, CORS);
    }

    return err(404, "No data available for symbol", CORS);

  } catch (error) {
    console.error("Error:", error);
    return err(500, error.message, CORS);
  }
};