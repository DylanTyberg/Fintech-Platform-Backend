import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

export const handler = async (event) => {
  const userId = event.queryStringParameters?.userId; 
  console.log(userId);

  


  const params = {
    TableName: "stock-user-data",
    KeyConditionExpression: "userId = :userId",
    ExpressionAttributeValues: {
      ":userId": userId
    }
  };

  try {
    const result = await dynamo.send(new QueryCommand(params));

    
    return {
      statusCode: 200,
      body: JSON.stringify(result.Items),
      headers: {
        'Access-Control-Allow-Origin': '*', 
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
    }
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify(error),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS'
      },
    }
  }
}