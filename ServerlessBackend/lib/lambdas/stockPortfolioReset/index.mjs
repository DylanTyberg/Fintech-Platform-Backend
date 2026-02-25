import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, BatchWriteCommand } from "@aws-sdk/lib-dynamodb";

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

  const { user } = params;

  try {
    // Query all items for this user
    const queryParams = {
      TableName: "stock-user-data",
      KeyConditionExpression: "userId = :userId",
      ExpressionAttributeValues: {
        ":userId": user
      }
    };

    const queryResult = await dynamo.send(new QueryCommand(queryParams));
    
    // Filter out items that start with "watchlist#"
    const itemsToDelete = queryResult.Items.filter(item => 
      !item.type.startsWith('watchlist#')
    );

    if (itemsToDelete.length === 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({ 
          message: 'No portfolio items to delete',
          deletedCount: 0
        }),
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Allow-Methods': 'DELETE,OPTIONS'
        },
      };
    }

    // DynamoDB BatchWrite can handle max 25 items at a time
    const batchSize = 25;
    let deletedCount = 0;

    for (let i = 0; i < itemsToDelete.length; i += batchSize) {
      const batch = itemsToDelete.slice(i, i + batchSize);
      
      const deleteRequests = batch.map(item => ({
        DeleteRequest: {
          Key: {
            userId: item.userId,
            type: item.type
          }
        }
      }));

      const batchParams = {
        RequestItems: {
          "stock-user-data": deleteRequests
        }
      };

      await dynamo.send(new BatchWriteCommand(batchParams));
      deletedCount += batch.length;
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ 
        message: 'Portfolio reset successfully',
        deletedCount: deletedCount,
        userId: user
      }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'DELETE,OPTIONS'
      },
    };

  } catch (error) {
    console.error('Portfolio reset error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ 
        error: error.message || 'Failed to reset portfolio'
      }),
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'DELETE,OPTIONS'
      },
    };
  }
};