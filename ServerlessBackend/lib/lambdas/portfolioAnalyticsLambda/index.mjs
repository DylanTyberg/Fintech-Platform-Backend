// ============================================================================
// GET /portfolio/analytics  (wire behind the same Cognito authorizer your
// other authenticated endpoints use — confirmed correct per your last message)
//
// Returns Sharpe, Volatility, Max Drawdown, Beta vs SPY, a rebased
// portfolio-vs-SPY comparison series, sector allocation, and performance
// leaders — for 1M / 3M / 1Y / All in a single response, so the frontend
// toggle switches periods instantly with no re-fetch.
//
// DEPLOYMENT NOTE: reads s&p500stocks.json from disk at runtime for the
// sector lookup. Include that file in this Lambda's deployment package
// (same folder as this index.mjs) — it is NOT read from the frontend copy.
//
// Uses BOTH DynamoDB client styles on purpose, matching your two existing
// Lambdas exactly:
//   - raw DynamoDBClient + {S:...}/{N:...} values  -> stock-app-data-daily
//   - DynamoDBDocumentClient + plain JS values      -> stock-user-data
// ============================================================================

import https from 'https';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { DynamoDBClient, QueryCommand as RawQueryCommand, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { ok, err } from "/opt/response.mjs";

const rawClient = new DynamoDBClient({});
const dynamodb = rawClient;                            // for DAILY_TABLE (raw .S/.N values)
const dynamo = DynamoDBDocumentClient.from(rawClient);  // for USERS_TABLE (plain JS values)

const DAILY_TABLE = process.env.DAILY_TABLE || 'stock-app-data-daily';
const USERS_TABLE = process.env.USER_DATA_TABLE || 'stock-user-data';

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,GET',
};

// Hardcoded, not fetched live — for a paper-trading app this is a policy
// choice (roughly current short-term Treasury yield), not something worth
// a live data feed for. Bump it if you want it more current.
const RISK_FREE_RATE = 0.04;

const PERIODS = { "1M": 30, "3M": 90, "1Y": 365, "All": Infinity };

// Symbols that should bucket as "Broad Index ETF" rather than "Other" in
// sector allocation, since they won't appear in the S&P 500 constituent
// sector list (they're not S&P 500 companies, they're funds).
const KNOWN_INDEX_ETFS = new Set(["SPY", "QQQ", "DIA", "IWM", "VOO", "VTI", "VEA", "VWO", "VXUS"]);

// ---------------------------------------------------------------------------
// Sector lookup — loaded once per cold start, not per request.
// ---------------------------------------------------------------------------
const __dirname = dirname(fileURLToPath(import.meta.url));
const sp500Data = JSON.parse(readFileSync(join(__dirname, 's&p500stocks.json'), 'utf-8'));
const sectorBySymbol = {};
for (const stock of sp500Data) {
  sectorBySymbol[stock.Symbol] = stock["GICS Sector"];
}

const sectorFor = (symbol) => {
  if (sectorBySymbol[symbol]) return sectorBySymbol[symbol];
  if (KNOWN_INDEX_ETFS.has(symbol)) return "Broad Index ETF";
  return "Other";
};

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------
const pad2 = (n) => String(n).padStart(2, "0");
const formatYMD = (date) => `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;

const daysAgoStr = (days) => {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return formatYMD(d);
};

// ---------------------------------------------------------------------------
// NYSE holiday calendar — ported from the frontend's marketHolidays.js so
// this Lambda applies the identical trading-day filter. Snapshots and daily
// bars taken on a closed-market day are unreliable (the snapshot job
// currently records cash-only on those days) — filtering here means every
// stat computed below is server-side-correct, not just the frontend chart.
// ---------------------------------------------------------------------------
const observedDate = (date) => {
  const day = date.getDay();
  if (day === 6) return new Date(date.getFullYear(), date.getMonth(), date.getDate() - 1);
  if (day === 0) return new Date(date.getFullYear(), date.getMonth(), date.getDate() + 1);
  return date;
};

const nthWeekdayOfMonth = (year, month, weekday, n) => {
  const first = new Date(year, month, 1);
  const offset = (weekday - first.getDay() + 7) % 7;
  return new Date(year, month, 1 + offset + (n - 1) * 7);
};

const lastWeekdayOfMonth = (year, month, weekday) => {
  const last = new Date(year, month + 1, 0);
  const offset = (last.getDay() - weekday + 7) % 7;
  return new Date(year, month, last.getDate() - offset);
};

const getEasterSunday = (year) => {
  const a = year % 19, b = Math.floor(year / 100), c = year % 100;
  const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4), k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(year, month - 1, day);
};

const getGoodFriday = (year) => {
  const easter = getEasterSunday(year);
  return new Date(easter.getFullYear(), easter.getMonth(), easter.getDate() - 2);
};

const holidaySetCache = {};
const getMarketHolidaySet = (year) => {
  if (holidaySetCache[year]) return holidaySetCache[year];
  const dates = [
    observedDate(new Date(year, 0, 1)),
    nthWeekdayOfMonth(year, 0, 1, 3),
    nthWeekdayOfMonth(year, 1, 1, 3),
    getGoodFriday(year),
    lastWeekdayOfMonth(year, 4, 1),
    observedDate(new Date(year, 5, 19)),
    observedDate(new Date(year, 6, 4)),
    nthWeekdayOfMonth(year, 8, 1, 1),
    nthWeekdayOfMonth(year, 10, 4, 4),
    observedDate(new Date(year, 11, 25)),
  ];
  const set = new Set(dates.map(formatYMD));
  holidaySetCache[year] = set;
  return set;
};

const isTradingDay = (ymdString) => {
  const [y, m, d] = ymdString.split("-").map(Number);
  const dayOfWeek = new Date(y, m - 1, d).getDay();
  if (dayOfWeek === 0 || dayOfWeek === 6) return false;
  return !getMarketHolidaySet(y).has(ymdString);
};

// ---------------------------------------------------------------------------
// Polygon fetch — same call shape as your existing daily-put function.
// ---------------------------------------------------------------------------
const fetchPolygonData = (symbol, from, to, apiKey) => {
  const url = `https://api.polygon.io/v2/aggs/ticker/${symbol}/range/1/day/${from}/${to}?adjusted=true&sort=asc&limit=50000&apiKey=${apiKey}`;
  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => {
        try {
          const data = JSON.parse(body);
          if ((data.status !== 'OK' && data.status !== 'DELAYED') || !data.results) {
            resolve({ results: [] }); // no data is not fatal here — just skip that symbol
          } else {
            resolve(data);
          }
        } catch (error) {
          reject(error);
        }
      });
    }).on('error', reject);
  });
};

// Ensures today's bar exists for a symbol before we read its history —
// mirrors the "check last date, fetch if stale" logic from your daily-put
// function, refactored so this Lambda doesn't depend on the user having
// visited that symbol's stock-details page first.
const ensureDailyDataFresh = async (symbol, apiKey) => {
  const today = formatYMD(new Date());

  const latest = await dynamodb.send(new RawQueryCommand({
    TableName: DAILY_TABLE,
    KeyConditionExpression: 'symbol = :symbol',
    ExpressionAttributeValues: { ':symbol': { S: symbol } },
    ScanIndexForward: false,
    Limit: 1,
  }));

  let fromDate;
  if (latest.Items.length > 0 && latest.Items[0].timestamp.S === today) {
    return; // already fresh
  } else if (latest.Items.length > 0) {
    fromDate = latest.Items[0].timestamp.S;
  } else {
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    fromDate = formatYMD(oneYearAgo);
  }

  try {
    const data = await fetchPolygonData(symbol, fromDate, today, apiKey);
    const bars = data.results || [];
    if (bars.length === 0) return;

    const items = bars.map((bar) => {
      const timestamp = new Date(bar.t);
      return {
        PutRequest: {
          Item: {
            symbol: { S: symbol },
            timestamp: { S: formatYMD(timestamp) },
            open: { N: bar.o.toString() },
            high: { N: bar.h.toString() },
            low: { N: bar.l.toString() },
            close: { N: bar.c.toString() },
            volume: { N: bar.v.toString() },
          }
        }
      };
    });

    for (let i = 0; i < items.length; i += 25) {
      const batch = items.slice(i, i + 25);
      await dynamodb.send(new BatchWriteItemCommand({ RequestItems: { [DAILY_TABLE]: batch } }));
    }
  } catch (error) {
    console.log(JSON.stringify({ message: `Failed refreshing daily data for ${symbol}`, error: error.message }));
    // Non-fatal — stats just compute on whatever history already exists.
  }
};

// Reads daily closes for a symbol going back `days`, filtered to trading
// days only, sorted ascending. Returns [{ time: "YYYY-MM-DD", value }].
const getDailySeries = async (symbol, days) => {
  const from = daysAgoStr(days === Infinity ? 3650 : days + 10); // pad a bit past the window
  const to = formatYMD(new Date());

  const result = await dynamodb.send(new RawQueryCommand({
    TableName: DAILY_TABLE,
    KeyConditionExpression: 'symbol = :symbol AND #ts BETWEEN :from AND :to',
    ExpressionAttributeNames: { '#ts': 'timestamp' },
    ExpressionAttributeValues: {
      ':symbol': { S: symbol },
      ':from': { S: from },
      ':to': { S: to },
    },
    ScanIndexForward: true,
  }));

  return result.Items
    .filter((item) => isTradingDay(item.timestamp.S))
    .map((item) => ({ time: item.timestamp.S, value: Number(item.close.N) }));
};

// ---------------------------------------------------------------------------
// User record — mirrors the exact filter/map logic your own frontend uses
// after calling GET /user (single-table design: userId partition key,
// type sort key like "holding#AAPL" / "cash#..." / "snapshot#MM-DD-YYYY").
// ---------------------------------------------------------------------------
const getUserRecord = async (userId) => {
  const result = await dynamo.send(new QueryCommand({
    TableName: USERS_TABLE,
    KeyConditionExpression: "userId = :userId",
    ExpressionAttributeValues: { ":userId": userId },
  }));

  const items = result.Items || [];

  const cash = items.find((item) => item.type?.startsWith("cash#"))?.amount || 0;

  const holdings = items
    .filter((item) => item.type?.startsWith("holding#") && item?.quantity > 0)
    .map((item) => ({
      symbol: item.type.split("#")[1],
      quantity: item.quantity,
    }));

  // totalPortfolioValue only — each snapshot item also carries cash and
  // holdings-at-that-time if you ever want true historical sector
  // allocation instead of current-only. Not used for these metrics.
  const snapshots = items
    .filter((item) => item.type?.startsWith("snapshot#"))
    .map((item) => ({
      date: item.type.split("#")[1],
      portfolioValue: item.totalPortfolioValue,
    }));

  return { cash, holdings, snapshots };
};

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------
const dailyReturns = (series) => {
  const returns = [];
  for (let i = 1; i < series.length; i++) {
    const prev = series[i - 1].value;
    const curr = series[i].value;
    if (prev > 0) returns.push((curr - prev) / prev);
  }
  return returns;
};

const mean = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);

const sampleStdev = (arr) => {
  if (arr.length < 2) return 0;
  const m = mean(arr);
  const variance = arr.reduce((sum, x) => sum + (x - m) ** 2, 0) / (arr.length - 1);
  return Math.sqrt(variance);
};

const annualizedVolatility = (returns) => sampleStdev(returns) * Math.sqrt(252);

const sharpeRatio = (returns, volatility) => {
  if (volatility === 0) return null;
  const annualizedReturn = mean(returns) * 252;
  return (annualizedReturn - RISK_FREE_RATE) / volatility;
};

// Requires paired, same-length, same-date-order return arrays.
const beta = (portfolioReturns, benchmarkReturns) => {
  const n = Math.min(portfolioReturns.length, benchmarkReturns.length);
  if (n < 2) return null;
  const p = portfolioReturns.slice(0, n);
  const s = benchmarkReturns.slice(0, n);
  const meanP = mean(p), meanS = mean(s);
  let cov = 0, varS = 0;
  for (let i = 0; i < n; i++) {
    cov += (p[i] - meanP) * (s[i] - meanS);
    varS += (s[i] - meanS) ** 2;
  }
  cov /= (n - 1);
  varS /= (n - 1);
  return varS === 0 ? null : cov / varS;
};

const maxDrawdown = (series) => {
  let peak = -Infinity, peakDate = null;
  let worstDD = 0, ddStart = null, ddEnd = null;
  for (const point of series) {
    if (point.value > peak) {
      peak = point.value;
      peakDate = point.time;
    }
    const dd = peak > 0 ? (point.value - peak) / peak : 0;
    if (dd < worstDD) {
      worstDD = dd;
      ddStart = peakDate;
      ddEnd = point.time;
    }
  }
  return { pct: worstDD * 100, start: ddStart, end: ddEnd };
};

// Keeps only dates present in both series (defensive — portfolio snapshot
// dates and SPY trading days should mostly match once both are filtered
// through isTradingDay, but this guards against any gap).
const alignSeries = (seriesA, seriesB) => {
  const mapB = new Map(seriesB.map((p) => [p.time, p.value]));
  const alignedA = [], alignedB = [];
  for (const point of seriesA) {
    if (mapB.has(point.time)) {
      alignedA.push(point);
      alignedB.push({ time: point.time, value: mapB.get(point.time) });
    }
  }
  return [alignedA, alignedB];
};

const sliceToPeriod = (series, days) => {
  if (days === Infinity) return series;
  const cutoff = daysAgoStr(days);
  return series.filter((p) => p.time >= cutoff);
};

// Rebases a series to "% change from first point" — what the comparison
// chart actually plots (portfolio % vs SPY %, both starting at 0).
const rebaseToPercent = (series) => {
  if (series.length === 0) return [];
  const base = series[0].value;
  if (base <= 0) return series.map((p) => ({ time: p.time, value: 0 }));
  return series.map((p) => ({ time: p.time, value: ((p.value - base) / base) * 100 }));
};

const computeSectorAllocation = (holdings, latestPriceBySymbol) => {
  const bySector = {};
  let total = 0;
  for (const h of holdings) {
    const price = latestPriceBySymbol[h.symbol];
    if (!price) continue;
    const value = h.quantity * price;
    const sector = sectorFor(h.symbol);
    bySector[sector] = (bySector[sector] || 0) + value;
    total += value;
  }
  return Object.entries(bySector)
    .map(([sector, value]) => ({ sector, pct: total > 0 ? (value / total) * 100 : 0 }))
    .sort((a, b) => b.pct - a.pct);
};

const computePerformanceLeaders = (holdings, dailySeriesBySymbol, days) => {
  const leaders = [];
  for (const h of holdings) {
    const series = dailySeriesBySymbol[h.symbol];
    if (!series || series.length < 2) continue;
    const periodSeries = sliceToPeriod(series, days);
    if (periodSeries.length < 2) continue;
    const startPrice = periodSeries[0].value;
    const endPrice = periodSeries[periodSeries.length - 1].value;
    if (startPrice <= 0) continue;
    leaders.push({
      symbol: h.symbol,
      pctChange: ((endPrice - startPrice) / startPrice) * 100,
    });
  }
  return leaders.sort((a, b) => b.pctChange - a.pctChange);
};

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------
export const handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return ok(null, 200, CORS);
  }

  const userId = event.requestContext?.authorizer?.claims?.sub;
  if (!userId) {
    return err(401, "Unauthorized", CORS);
  }

  const apiKey = process.env.POLYGON_API_KEY;

  try {
    const { cash, holdings, snapshots } = await getUserRecord(userId);

    // Portfolio value history, trading-days only, sorted ascending.
    const snapshotSeries = snapshots
      .map((s) => {
        const [month, day, year] = s.date.split("-");
        return { time: `${year}-${month}-${day}`, value: s.portfolioValue };
      })
      .filter((p) => isTradingDay(p.time))
      .sort((a, b) => a.time.localeCompare(b.time));

    // Refresh daily data for SPY + every held symbol before reading history.
    const symbols = [...new Set(["SPY", ...holdings.map((h) => h.symbol)])];
    await Promise.all(symbols.map((symbol) => ensureDailyDataFresh(symbol, apiKey)));

    // Pull ~13 months of daily closes for everything — covers 1Y with room
    // to compute the first day's return, "All" is bounded by snapshot
    // history instead (that's inherently the shorter series for most
    // accounts, same as PortfolioAnalytics.jsx already assumes).
    const LOOKBACK_DAYS = 400;
    const spySeriesFull = await getDailySeries("SPY", LOOKBACK_DAYS);
    const dailySeriesBySymbol = {};
    for (const symbol of holdings.map((h) => h.symbol)) {
      dailySeriesBySymbol[symbol] = await getDailySeries(symbol, LOOKBACK_DAYS);
    }

    const latestPriceBySymbol = {};
    for (const [symbol, series] of Object.entries(dailySeriesBySymbol)) {
      if (series.length > 0) latestPriceBySymbol[symbol] = series[series.length - 1].value;
    }

    const periods = {};
    for (const [label, days] of Object.entries(PERIODS)) {
      const portfolioSlice = sliceToPeriod(snapshotSeries, days);
      const spySlice = sliceToPeriod(spySeriesFull, days);
      const [alignedPortfolio, alignedSpy] = alignSeries(portfolioSlice, spySlice);

      if (alignedPortfolio.length < 2) {
        periods[label] = null; // not enough history yet for this window
        continue;
      }

      const portfolioReturns = dailyReturns(alignedPortfolio);
      const spyReturns = dailyReturns(alignedSpy);
      const volatility = annualizedVolatility(portfolioReturns);
      const spyVolatility = annualizedVolatility(spyReturns);

      const portfolioReturnPct =
        ((alignedPortfolio[alignedPortfolio.length - 1].value - alignedPortfolio[0].value) /
          alignedPortfolio[0].value) * 100;
      const spyReturnPct =
        ((alignedSpy[alignedSpy.length - 1].value - alignedSpy[0].value) /
          alignedSpy[0].value) * 100;

      periods[label] = {
        portfolioReturnPct,
        spyReturnPct,
        sharpeRatio: sharpeRatio(portfolioReturns, volatility),
        spySharpeRatio: sharpeRatio(spyReturns, spyVolatility),
        volatilityPct: volatility * 100,
        spyVolatilityPct: spyVolatility * 100,
        beta: beta(portfolioReturns, spyReturns),
        maxDrawdown: maxDrawdown(alignedPortfolio),
        chartSeries: {
          portfolio: rebaseToPercent(alignedPortfolio),
          spy: rebaseToPercent(alignedSpy),
        },
        performanceLeaders: computePerformanceLeaders(holdings, dailySeriesBySymbol, days),
      };
    }

    const sectorAllocation = computeSectorAllocation(holdings, latestPriceBySymbol);

    return ok({ periods, sectorAllocation, cash }, 200, CORS);
  } catch (error) {
    console.log(JSON.stringify({ message: "Error computing portfolio analytics", error: error.message, stack: error.stack }));
    return err(500, "Failed to compute portfolio analytics", CORS);
  }
};