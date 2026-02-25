import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

export const handler = async (event) => {
  
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,PUT'
      },
      body: ''
    };
  }
  
  const params = JSON.parse(event.body);
  console.log(params);

  const { user, type, details, ...remaining } = params;
  
  const sortKey = `${type}#${details}`

  


  const item = {
    TableName: "stock-user-data",
    Item: {
      userId: user,
      type: sortKey,
      ...remaining
    }
  }

  try {
    await dynamo.send(new PutCommand(item));
    
    return {
      statusCode: 200,
      body: JSON.stringify(item.Item),
      headers: {
        'Access-Control-Allow-Origin': '*', 
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,PUT'
      },
    }
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify(error),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,PUT'
      },
    }
  }
}