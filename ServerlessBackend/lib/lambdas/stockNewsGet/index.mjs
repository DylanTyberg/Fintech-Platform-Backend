// GET /news?symbols=AAPL,MSFT&type=positive
// Fetches news from Marketaux with caching in DynamoDB (1 hour TTL).
//
// type options: positive, negative, neutral, recent
// Marketaux sentiment filter: gt (greater than) / lt (less than) sentiment score

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { ok, err } from "/opt/response.mjs";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const MARKETAUX_API_KEY = "OmsRbojVnf7zaJYADxuFEC8LagnlEjUcWz9jWDBT";
const NEWS_TABLE = "stock-app-data-news";

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET,OPTIONS',
};

const SENTIMENT_FILTERS = {
  positive: { must_have_entities: true, sentiment_gte: 0.3  },
  negative: { must_have_entities: true, sentiment_lte: -0.3 },
  neutral:  { must_have_entities: true, sentiment_gte: -0.1, sentiment_lte: 0.1 },
  recent:   { must_have_entities: true },
};

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return ok(null, 200, CORS);
  }

  const { symbols, type = 'recent' } = event.queryStringParameters ?? {};

  if (!symbols) return err(400, 'Missing query param: symbols', CORS);
  if (!SENTIMENT_FILTERS[type]) {
    return err(400, `Invalid type. Must be one of: ${Object.keys(SENTIMENT_FILTERS).join(', ')}`, CORS);
  }

  // Normalize symbols — sort and uppercase for consistent cache key
  const symbolList = symbols.split(',').map(s => s.trim().toUpperCase()).sort();
  const cacheKey = `${symbolList.join(',')}_${type}`;

  // Check cache
  try {
    const cached = await ddb.send(new GetCommand({
      TableName: NEWS_TABLE,
      Key: { cacheKey },
    }));

    if (cached.Item && cached.Item.ttl > Math.floor(Date.now() / 1000)) {
      console.log(`Cache hit: ${cacheKey}`);
      return ok(cached.Item.data, 200, CORS);
    }
  } catch (error) {
    console.warn('Cache read error:', error.message);
  }

  // Cache miss — fetch from Marketaux
  try {
    const filters = SENTIMENT_FILTERS[type];
    const params = new URLSearchParams({
      api_token: MARKETAUX_API_KEY,
      symbols: symbolList.join(','),
      limit: '10',
      language: 'en',
      ...filters,
    });

    const url = `https://api.marketaux.com/v1/news/all?${params.toString()}`;
    const data = await fetchJson(url);

    // Write to cache with 1 hour TTL
    await ddb.send(new PutCommand({
      TableName: NEWS_TABLE,
      Item: {
        cacheKey,
        data,
        ttl: Math.floor(Date.now() / 1000) + 3600,
        cachedAt: new Date().toISOString(),
      },
    }));

    return ok(data, 200, CORS);
  } catch (error) {
    console.error('Marketaux fetch error:', error);
    return err(500, 'Failed to fetch news', CORS);
  }
};

const fetchJson = (url) => new Promise((resolve, reject) => {
  import('https').then(({ default: https }) => {
    https.get(url, (r) => {
      let body = '';
      r.on('data', c => body += c);
      r.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
});