import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { PutCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const getAllUserIds = async () => {
    let params = {
      TableName: "stock-user-data",
      ProjectionExpression: "userId", 
    };
  
    let userIds = new Set();
    let lastEvaluatedKey = undefined;
  
    do {
      const command = new ScanCommand({
        ...params,
        ExclusiveStartKey: lastEvaluatedKey,
      });
      const data = await dynamo.send(command);
      
      for (const item of data.Items) {
        if (item.userId) {
          userIds.add(item.userId);
        }
      }
  
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (lastEvaluatedKey);
  
    const uniqueUserIds = Array.from(userIds);
    console.log("Unique userIds:", uniqueUserIds);
    return uniqueUserIds;
}

const invokeOtherLambda = async (holdings) => {
  try {
    const response = await fetch(
      "https://as9ppqd9d8.execute-api.us-east-1.amazonaws.com/dev/intraday/holdings-prices",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ holdings: holdings }), 
      }
    );

    const result = await response.json();
    
    if (!response.ok) {
      console.error("API Error Response:", result);
      throw new Error(`HTTP error! status: ${response.status}, body: ${JSON.stringify(result)}`);
    }
    
    console.log("price info", result);
    return result;
  } catch (error) {
    console.error("Error calling API:", error);
    console.error("Holdings sent:", holdings);
    throw error;
  }
};

const calculatePortfolioValue = async (priceData, holdings, userId) => {
  let totalValue = 0;
  const holdingsWithValue = [];

  // Create a map of holdings for easy lookup
  const holdingsMap = holdings.reduce((map, holding) => {
    map[holding.symbol] = holding.quantity;
    return map;
  }, {});

  // Calculate value for each holding
  priceData.forEach(stock => {
    const quantity = holdingsMap[stock.symbol] || 0;
    const lastPrice = stock.lastPrice?.close || 0;
    const holdingValue = lastPrice * quantity;
    
    totalValue += holdingValue;
    
    holdingsWithValue.push({
      symbol: stock.symbol,
      quantity: quantity,
      lastPrice: lastPrice,
      holdingValue: holdingValue,
      change: stock.change
    });
  });

  const cash_params = {
    TableName: "stock-user-data",
    KeyConditionExpression: "userId = :userId AND begins_with(#type, :prefix)",
    ExpressionAttributeNames: {
      "#type": "type"
    },
    ExpressionAttributeValues: {
      ":userId": userId,
      ":prefix": "cash#"
    },
    Limit: 1
    
  }
  const cash_result = await dynamo.send(new QueryCommand(cash_params)); 
  console.log("cash result", cash_result);
  const cash = Number(cash_result.Items[0].amount); 
  totalValue += cash;

  return {
    totalPortfolioValue: totalValue,
    cash: cash, 
    holdings: holdingsWithValue
  };
};

const getPrice = async (userId) => {
  const params = {
    TableName: "stock-user-data",
    KeyConditionExpression: "userId = :userId AND begins_with(#type, :prefix)",
    ExpressionAttributeNames: {
      "#type": "type"
    },
    ExpressionAttributeValues: {
      ":userId": userId,
      ":prefix": "holding#" 
    }
  }

  console.log("Processing userId:", userId);
  const result = await dynamo.send(new QueryCommand(params));
  
  if (!result.Items || result.Items.length === 0) {
    console.log("No holdings found for user:", userId);
    return null;
  }
  
  const holdings = result.Items.map(item => {
    const symbol = item.type.split("#")[1];
    const quantity = Number(item.quantity);
    return { symbol, quantity };
  });

  console.log("holdings", holdings);
  
  if (holdings.length === 0) {
    console.log("Holdings array is empty");
    return null;
  }

  const priceData = await invokeOtherLambda(holdings);
  console.log("price data received:", priceData);
  
  const portfolioSummary = await calculatePortfolioValue(priceData, holdings, userId);
  console.log("Portfolio summary:", portfolioSummary);
  
  const today = new Date();

  let day = today.getDate();
  let month = today.getMonth() + 1; // getMonth() returns 0-indexed month
  let year = today.getFullYear();

  // Add leading zeros if day or month is less than 10
  day = day < 10 ? '0' + day : day;
  month = month < 10 ? '0' + month : month;

  const formattedDate = `${month}-${day}-${year}`;
  

  return {
    userId: userId,
    type: "snapshot#" + formattedDate,
    ...portfolioSummary
  };
}

export const handler = async () => {
  const userIds = await getAllUserIds();

  const promises = userIds.map(async (userId) => {
    try {
      const portfolioData = await getPrice(userId);
      return portfolioData;
    } catch (error) {
      console.error(`Error processing user ${userId}:`, error);
      return null;
    }
  });

  const allPortfolios = await Promise.all(promises);
  
  // Filter out null results
  const validPortfolios = allPortfolios.filter(portfolio => portfolio !== null);
  
  const put_promises = validPortfolios.map(async (portfolio) => {
    const params = {
      TableName: "stock-user-data",
      Item: portfolio
    };

    try {
      const command = new PutCommand(params);
      await dynamo.send(command);
      console.log(`Successfully put portfolio for user ${portfolio.userId}`);
    } catch (error) {
      console.error(`Error putting portfolio for user ${portfolio.userId}:`, error);
    }
  });

  return {
    statusCode: 200,
    body: JSON.stringify(validPortfolios)
  };
}