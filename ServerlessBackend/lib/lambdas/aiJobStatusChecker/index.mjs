import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand } from "@aws-sdk/lib-dynamodb";
import { ok, err } from "/opt/response.mjs";

const dynamoClient = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(dynamoClient);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET,OPTIONS',
};

export const handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return ok(null, 200, CORS);
  }

  const jobId = event.pathParameters?.jobId;

  if (!jobId) {
    return err(400, "Missing jobId", CORS);
  }

  const result = await dynamo.send(new GetCommand({
    TableName: "ai-jobs",
    Key: { jobId },
  }));

  if (!result.Item) {
    return err(404, "Job not found", CORS);
  }

  const { jobId: id, status, result: jobResult, error, createdAt, completedAt } = result.Item;

  return ok({ jobId: id, status, result: jobResult, error, createdAt, completedAt }, 200, CORS);
};