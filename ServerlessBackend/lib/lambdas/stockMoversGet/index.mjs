import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

export const handler = async () => {

  

  const params = {
    
    TableName: "stock-app-data-movers",

  }

  try {
    const data = await dynamo.send(new ScanCommand(params));
    console.log(data.Items);
    return {
      statusCode: 200,
      body: JSON.stringify(data.Items),
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