import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { DynamoDBClient} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, UpdateCommand} from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);

const getPortfolioFromDB = async (userId) => {
  if (!userId) return ["this user is not signed in."];
  const params = {
    TableName: "stock-user-data",
    KeyConditionExpression: "userId = :userId",
    ExpressionAttributeValues: {
      ":userId": userId
    }
  }

  const data = await dynamo.send(new QueryCommand(params));
  return data.Items
};

const getIntradayStockPrices = async (symbols) => {
  const response = await fetch('https://as9ppqd9d8.execute-api.us-east-1.amazonaws.com/dev/intraday/list', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ symbols: symbols })
  });
  
  const data = await response.json();
  return data.results;
};

const getStockPrices = async (symbols) => {
  const response = await fetch('https://as9ppqd9d8.execute-api.us-east-1.amazonaws.com/dev/daily/list', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ symbols: symbols })
  });
  
  const data = await response.json();
  return data.results;
};

export const handler = async (event) => {
  // NEW: Get jobId and data from async invocation
  const jobId = event.jobId;
  const userId = event.userId;
  const contextPrompts = event.prompts || [];
  const responses = event.responses || [];
  const newPrompt = event.prompt;

  try {
    const bedrockClient = new BedrockRuntimeClient({ region: "us-east-1" });
    const portfolio = await getPortfolioFromDB(userId);

    const tools = [
      {
        name: "get_stock_prices",
        description: "Gets historical daily close prices of 30 popular individual stocks, 4 US indices, and sector ETFs, for about the last year.",
        input_schema: {
          type: "object",
          properties: {
            symbols: {
              type: "array",
              items: { type: "string" },
              description: "Array of stock ticker symbols like ['AAPL', 'MSFT', 'GOOGL']"
            }
          },
          required: ["symbols"]
        }
      },
      {
        name: "get_intraday_stock_prices",
        description: "Gets most recent minute-by-minute intraday prices of popular stocks, 4 US indices, and sector ETFs.",
        input_schema: {
          type: "object",
          properties: {
            symbols: {
              type: "array",
              items: { type: "string" },
              description: "Array of stock ticker symbols like ['AAPL', 'MSFT', 'GOOGL']"
            }
          },
          required: ["symbols"]
        }
      },

    ];

    // Build conversation history (YOUR EXISTING CODE - NO CHANGES)
    let messages = [];

    for (let i = 0; i < Math.min(contextPrompts.length, responses.length); i++) {
      messages.push({
        role: "user",
        content: contextPrompts[i]
      });
      messages.push({
        role: "assistant",
        content: responses[i]
      });
    }
    
    messages.push({
      role: "user",
      content: `You are an AI financial advisor assistant with access to real-time market data. 
    
    Context:
    - User's portfolio: ${JSON.stringify(portfolio, null, 2)}
    - Current date: ${new Date().toLocaleDateString()}
    - You have access to tools to fetch stock price data for analysis
    
    Guidelines:
    - Provide specific, actionable advice based on the user's actual holdings
    - Use the get_stock_prices tool when you need current market data
    - Consider risk, diversification, and the user's portfolio composition
    - Support your recommendations with data when possible
    - Cross Check that latest snapshot matches the watchlist and holdings data before relying on it
    
    User's question: ${newPrompt}`
    });

    // YOUR EXISTING BEDROCK LOOP - NO CHANGES
    let finalResponse;
    let loopCount = 0;
    const MAX_LOOPS = 5;

    while (loopCount < MAX_LOOPS) {
      const command = new InvokeModelCommand({
        modelId: "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        contentType: "application/json",
        accept: "application/json",
        body: JSON.stringify({
          anthropic_version: "bedrock-2023-05-31",
          max_tokens: 2000,
          tools: tools,
          messages: messages
        })
      });

      const response = await bedrockClient.send(command);
      const responseBody = JSON.parse(new TextDecoder().decode(response.body));

      if (responseBody.stop_reason === "tool_use") {
        const toolUse = responseBody.content.find(block => block.type === "tool_use");
        
        let toolResult;
        if (toolUse.name === "get_stock_prices") {
          toolResult = await getStockPrices(toolUse.input.symbols);
        }

        messages.push({
          role: "assistant",
          content: responseBody.content
        });

        messages.push({
          role: "user",
          content: [
            {
              type: "tool_result",
              tool_use_id: toolUse.id,
              content: JSON.stringify(toolResult)
            }
          ]
        });

        loopCount++;
      } else {
        finalResponse = responseBody.content.find(block => block.type === "text")?.text;
        break;
      }
    }

    // NEW: Save success result to DynamoDB
    // CORRECT - add result to ExpressionAttributeNames:
await dynamo.send(new UpdateCommand({
  TableName: "ai-jobs",
  Key: { jobId: jobId },
  UpdateExpression: "SET #status = :status, #result = :result, completedAt = :completedAt",
  ExpressionAttributeNames: { 
    "#status": "status",
    "#result": "result"  // ADD THIS LINE
  },
  ExpressionAttributeValues: {
    ":status": "COMPLETED",
    ":result": finalResponse || "Unable to generate response",
    ":completedAt": new Date().toISOString()
  }
}));

  } catch (error) {
    // NEW: Save error to DynamoDB
    console.error("Error:", error);
    await dynamo.send(new UpdateCommand({
      TableName: "ai-jobs",
      Key: { jobId: jobId },
      UpdateExpression: "SET #status = :status, #error = :error, completedAt = :completedAt",
      ExpressionAttributeNames: { 
        "#status": "status",
        "#error": "error"
      },
      ExpressionAttributeValues: {
        ":status": "FAILED",
        ":error": error.message,
        ":completedAt": new Date().toISOString()
      }
    }));
    throw error;
  }
};