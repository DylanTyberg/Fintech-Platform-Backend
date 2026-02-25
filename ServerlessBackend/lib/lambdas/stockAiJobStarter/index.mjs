// starter.js
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";

const dynamoClient = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(dynamoClient);
const lambdaClient = new LambdaClient({});

export const handler = async (event) => {
  console.log("Event received:", JSON.stringify(event)); // DEBUG
  
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
    };
  }

  try {
    const body = JSON.parse(event.body);
    console.log("Parsed body:", body); // DEBUG
    
    const jobId = randomUUID();
    console.log("Generated jobId:", jobId); // DEBUG

    // Store initial job status
    await dynamo.send(new PutCommand({
      TableName: "ai-jobs",
      Item: {
        jobId: jobId,
        status: "PROCESSING",
        userId: body.userId,
        prompt: body.prompt,
        prompts: body.prompts || [],
        responses: body.responses || [],
        createdAt: new Date().toISOString(),
        ttl: Math.floor(Date.now() / 1000) + 86400
      }
    }));
    console.log("Saved to DynamoDB"); // DEBUG

    // Invoke processor Lambda
    await lambdaClient.send(new InvokeCommand({
      FunctionName: process.env.PROCESSOR_LAMBDA_NAME || "Stock-AI-Insight-Portfolio-Suggestions",
      InvocationType: "Event",
      Payload: JSON.stringify({
        jobId: jobId,
        userId: body.userId,
        prompt: body.prompt,
        prompts: body.prompts || [],
        responses: body.responses || []
      })
    }));
    console.log("Invoked processor Lambda"); // DEBUG

    return {
      statusCode: 202,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
      body: JSON.stringify({
        jobId: jobId,
        status: "PROCESSING",
        message: "Job submitted successfully"
      })
    };
    
  } catch (error) {
    console.error("Error in starter lambda:", error); // DEBUG
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
      },
      body: JSON.stringify({
        error: error.message,
        details: error.stack
      })
    };
  }
};