import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

export const handler = async (event) => {
  
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'DELETE,OPTIONS'
      },
      body: ''
    };
  }
  
  const params = JSON.parse(event.body);
  console.log(params);

  const { user, type, symbol } = params;
  
  // Construct the sort key in the same format as your PUT lambda
  const sortKey = `${type}#${symbol}`;

  const deleteParams = {
    TableName: "stock-user-data",
    Key: {
      userId: user,
      type: sortKey
    }
  };

  try {
    await dynamo.send(new DeleteCommand(deleteParams));
    
    return {
      statusCode: 200,
      body: JSON.stringify({ 
        message: 'Item deleted successfully',
        userId: user,
        type: sortKey
      }),
      headers: {
        'Access-Control-Allow-Origin': '*', 
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'DELETE,OPTIONS'
      },
    };
  } catch (error) {
    console.error('Delete error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ 
        error: error.message || 'Failed to delete item'
      }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'DELETE,OPTIONS'
      },
    };
  }
};