// starter.js
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";
import { ok, err } from "/opt/response.mjs";

const dynamoClient = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(dynamoClient);
const lambdaClient = new LambdaClient({});

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'POST,OPTIONS',
};

const RATE_LIMIT = 5;
const WINDOW_MS = 5 * 60 * 1000;

// Add this function at the top of starter.js
const extractSubFromToken = (authHeader) => {
  if (!authHeader) return null;
  try {
    const token = authHeader.replace('Bearer ', '');
    const payload = token.split('.')[1];
    const decoded = JSON.parse(
      Buffer.from(payload, 'base64url').toString('utf8')
    );
    return decoded.sub || null;
  } catch {
    return null;
  }
};

export const handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return ok(null, 200, CORS);
  }

  try {
    const callerIp = event.requestContext?.identity?.sourceIp ?? 'unknown';
    const windowStart = new Date(Date.now() - WINDOW_MS).toISOString();

    // Rate limiting — unchanged, already good
    const recentJobs = await dynamo.send(new QueryCommand({
      TableName: process.env.AI_JOBS_TABLE,
      IndexName: "callerIp-createdAt-index",
      KeyConditionExpression: "callerIp = :ip AND createdAt > :windowStart",
      ExpressionAttributeValues: {
        ":ip": callerIp,
        ":windowStart": windowStart,
      },
      Select: "COUNT",
    }));

    if (recentJobs.Count >= RATE_LIMIT) {
      return err(429, `Rate limit exceeded. Maximum ${RATE_LIMIT} AI requests per 5 minutes.`, CORS);
    }

    const body = JSON.parse(event.body);

    // Extract userId from JWT claims — never trust client-supplied userId
    // Will be null for unauthenticated users (public endpoint)
    const userSub = extractSubFromToken(
      event.headers?.Authorization || event.headers?.authorization
    );

    // Validate prompt exists
    if (!body.prompt || typeof body.prompt !== 'string' || body.prompt.trim() === '') {
      return err(400, 'prompt is required', CORS);
    }

    const jobId = randomUUID();

    // Store job — no client-supplied history or userId
    await dynamo.send(new PutCommand({
      TableName: process.env.AI_JOBS_TABLE,
      Item: {
        jobId,
        status: "PROCESSING",
        userSub,                          // from JWT, not client
        prompt: body.prompt.trim(),
        sessionId: body.sessionId || null, // for conversation continuity
        createdAt: new Date().toISOString(),
        callerIp,
        ttl: Math.floor(Date.now() / 1000) + 86400,
      }
    }));

    // Invoke processor — no prompts/responses passed, history loaded server-side
    await lambdaClient.send(new InvokeCommand({
      FunctionName: process.env.INSIGHT_SUGGESTIONS_FUNCTION_NAME,
      InvocationType: "Event",
      Payload: JSON.stringify({
        jobId,
        userSub,                          // from JWT claims
        prompt: body.prompt.trim(),
        sessionId: body.sessionId || null,
        // prompts/responses intentionally removed — loaded from DynamoDB in processor
      })
    }));

    return ok({ jobId, status: "PROCESSING", message: "Job submitted successfully" }, 202, CORS);

  } catch (error) {
    console.error("Error in starter lambda:", error);
    return err(500, 'Internal server error', CORS);
  }
};