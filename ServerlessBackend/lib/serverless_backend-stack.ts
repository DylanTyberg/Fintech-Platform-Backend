import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as cloudtrail from 'aws-cdk-lib/aws-cloudtrail';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as logs from 'aws-cdk-lib/aws-logs';

export interface StockAppStackProps extends cdk.StackProps {
  polygonApiKey?: string;
}

export class StockAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: StockAppStackProps) {
    super(scope, id, props);

    // -----------------------------------------------------------------------
    // CLOUDTRAIL — Audit logging
    // -----------------------------------------------------------------------
    const trailBucket = new s3.Bucket(this, 'CloudTrailBucket', {
      bucketName: `cloudtrail-logs-${this.account}`,
      enforceSSL: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      lifecycleRules: [{
        expiration: cdk.Duration.days(365),  
      }],
      removalPolicy: cdk.RemovalPolicy.RETAIN, 
    });

    const trail = new cloudtrail.Trail(this, 'AuditTrail', {
      trailName: 'fintech-platform-audit',
      bucket: trailBucket,
      isMultiRegionTrail: true,
      enableFileValidation: true,        
      includeGlobalServiceEvents: true, 
      sendToCloudWatchLogs: true,     
      cloudWatchLogsRetention: logs.RetentionDays.ONE_YEAR,
    });

    // -----------------------------------------------------------------------
    // 1. IMPORT EXISTING RESOURCES
    // -----------------------------------------------------------------------

    // Cognito User Pool (existing)


    // DynamoDB Tables (existing — all imported, not created)
    const marketCacheTable = dynamodb.Table.fromTableName(
      this, 'MarketCacheTable', 'stock-app-data'
    );
    const dailyCacheTable = dynamodb.Table.fromTableName(
      this, 'DailyCacheTable', 'stock-app-data-daily'
    );
    const moversTable = dynamodb.Table.fromTableName(
      this, 'MoversTable', 'stock-app-data-movers'
    );
    const userDataTable = dynamodb.Table.fromTableName(
      this, 'UserDataTable', 'stock-user-data'
    );
    const aiJobsTable = dynamodb.Table.fromTableName(
      this, 'AiJobsTable', 'ai-jobs'
    );
    const newsTable = dynamodb.Table.fromTableName(
      this, 'NewsTable', 'stock-app-data-news'
    );

    // -----------------------------------------------------------------------
    // 2. S3 — Static hosting for React Frontend
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // 3. SHARED LAMBDA CONFIG
    // -----------------------------------------------------------------------

    const sharedLayer = new lambda.LayerVersion(this, 'SharedUtilsLayer', {
      code: lambda.Code.fromAsset(path.join(__dirname, 'lambdas/shared')),
      compatibleRuntimes: [lambda.Runtime.NODEJS_20_X],
      description: 'Shared response helpers',
    });

    // Table name constants — referenced per-function below
    const TABLE = {
      MARKET:    marketCacheTable.tableName,
      DAILY:     dailyCacheTable.tableName,
      MOVERS:    moversTable.tableName,
      USER:      userDataTable.tableName,
      AI_JOBS:   aiJobsTable.tableName,
      NEWS:      newsTable.tableName,
    };

    const commonProps = {
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      layers: [sharedLayer],
      // ← no shared environment block — each function gets only what it needs
    };

    const mkLambda = (
      id: string,
      functionName: string,
      handlerDir: string,
      overrides: Partial<lambda.FunctionProps> = {}
    ) =>
      new lambda.Function(this, id, {
        ...commonProps,
        functionName: `${functionName}-v2`,
        handler: 'index.handler',
        code: lambda.Code.fromAsset(path.join(__dirname, `lambdas/${handlerDir}`)),
        ...overrides,
      } as lambda.FunctionProps);

    // -----------------------------------------------------------------------
    // 4. USER LAMBDAS
    // -----------------------------------------------------------------------
    const stockUsersGetFn = mkLambda(
      'StockUsersGet', 'stock-users-get', 'stockUsersGet', {
      environment: {
        USER_DATA_TABLE: TABLE.USER,
      }
    });
    userDataTable.grantReadData(stockUsersGetFn);

    const stockUsersPutFn = mkLambda(
      'StockUsersPut', 'stock-users-put', 'stockUsersPut', {
      environment: {
        USER_DATA_TABLE: TABLE.USER,
      }
    });
    stockUsersPutFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:PutItem'],
      resources: [userDataTable.tableArn],
    }));

    const stockWatchlistDeleteFn = mkLambda(
      'StockWatchlistDelete', 'stock-watchlist-delete', 'stockWatchlistDelete', {
      environment: {
        USER_DATA_TABLE: TABLE.USER,
      }
    });
    stockWatchlistDeleteFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:DeleteItem'],
      resources: [userDataTable.tableArn],
    }));

    const stockPortfolioResetFn = mkLambda(
      'StockPortfolioReset', 'stock-portfolio-reset', 'stockPortfolioReset', {
      environment: {
        USER_DATA_TABLE: TABLE.USER,
      }
    });
    stockPortfolioResetFn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'dynamodb:Query',         // reads all items for the user
        'dynamodb:BatchWriteItem', // deletes portfolio items in batches
      ],
      resources: [
        userDataTable.tableArn,
      ],
    }));

    const stockPortfolioAnalyticsFn = mkLambda(
      'StockPortfolioAnalytics', 'portfolio-analytics', 'portfolioAnalyticsLambda', {
      environment: {
        USER_DATA_TABLE: TABLE.USER,
        DAILY_TABLE: TABLE.DAILY,
      }
    });
    userDataTable.grantReadData(stockPortfolioAnalyticsFn);
    dailyCacheTable.grantReadWriteData(stockPortfolioAnalyticsFn); // Query for history, BatchWriteItem via ensureDailyDataFresh

    // -----------------------------------------------------------------------
    // 5. INTRADAY LAMBDAS
    // -----------------------------------------------------------------------
    const stockHoldingsChangeGetFn = mkLambda(
      'StockHoldingsChangeGet', 'stock-holdings-change-get', 'stockHoldingsChangeGet', {
      environment: {
        MARKET_CACHE_TABLE: TABLE.MARKET,
        USER_DATA_TABLE:    TABLE.USER,
      }
    });
    marketCacheTable.grantReadData(stockHoldingsChangeGetFn);
    userDataTable.grantReadData(stockHoldingsChangeGetFn);

    const stockIntradayPutFn = mkLambda(
      'StockIntradayPut', 'stock-intraday-put', 'stockIntradayPut', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        POLYGON_API_KEY:    props.polygonApiKey ?? '',
        MARKET_CACHE_TABLE: TABLE.MARKET,
      }
    });
    marketCacheTable.grantReadWriteData(stockIntradayPutFn);

    const stockLatestPriceGetFn = mkLambda(
      'StockLatestPriceGet', 'stock-latest-price-get', 'stockLatestPriceGet', {
      environment: {
        MARKET_CACHE_TABLE: TABLE.MARKET,
        // INTRADAY_PUT_FUNCTION_NAME added below after stockIntradayPutFn is declared
      }
    });
    marketCacheTable.grantReadData(stockLatestPriceGetFn);
    stockLatestPriceGetFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [stockIntradayPutFn.functionArn],
    }));
    stockLatestPriceGetFn.addEnvironment(
      'INTRADAY_PUT_FUNCTION_NAME',
      stockIntradayPutFn.functionName
    );

    const stockIntradaySparklineFn = mkLambda(
      'StockIntradaySparkline', 'stock-intraday-sparkline', 'stockIntradaySparkline', {
      environment: {
        MARKET_CACHE_TABLE: TABLE.MARKET,
      }
    });
    marketCacheTable.grantReadData(stockIntradaySparklineFn);

    const stockIntradayListGetFn = mkLambda(
      'StockIntradayListGet', 'stock-intraday-list-get', 'stockIntradayListGet', {
      environment: {
        MARKET_CACHE_TABLE: TABLE.MARKET,
        USER_DATA_TABLE:    TABLE.USER,
      }
    });
    marketCacheTable.grantReadData(stockIntradayListGetFn);
    userDataTable.grantReadData(stockIntradayListGetFn);

    // -----------------------------------------------------------------------
    // 6. MOVERS LAMBDAS
    // -----------------------------------------------------------------------
    const stockMoversGetFn = mkLambda(
      'StockMoversGet', 'stock-movers-get', 'stockMoversGet', {
      environment: {
        MOVERS_TABLE: TABLE.MOVERS,
      }
    });
    moversTable.grantReadData(stockMoversGetFn);

    const stockMoversPutFn = mkLambda(
      'StockMoversPut', 'stock-movers-put', 'stockMoversPut', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        POLYGON_API_KEY: props.polygonApiKey ?? '',
        MOVERS_TABLE:    TABLE.MOVERS,
      }
    });
    moversTable.grantReadWriteData(stockMoversPutFn);

    // -----------------------------------------------------------------------
    // NEWS LAMBDA
    // -----------------------------------------------------------------------
    const stockNewsGetFn = mkLambda(
      'StockNewsGet', 'stock-news-get', 'stockNewsGet', {
      environment: {
        MARKETAUX_API_KEY: process.env.MARKETAUX_API_KEY ?? '',
        NEWS_TABLE:        TABLE.NEWS,
      }
    });
    newsTable.grantReadWriteData(stockNewsGetFn);

    // -----------------------------------------------------------------------
    // 7. DAILY LAMBDAS
    // -----------------------------------------------------------------------
    const stockDailyPutFn = mkLambda(
      'StockDailyPut', 'stock-daily-put', 'stockDailyPut', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        POLYGON_API_KEY:   props.polygonApiKey ?? '',
        DAILY_CACHE_TABLE: TABLE.DAILY,
      }
    });
    dailyCacheTable.grantReadWriteData(stockDailyPutFn);

    const stockDailyGetFn = mkLambda(
      'StockDailyGet', 'stock-daily-get', 'stockDailyGet', {
      environment: {
        DAILY_CACHE_TABLE: TABLE.DAILY,
        USER_DATA_TABLE:   TABLE.USER,
      }
    });
    dailyCacheTable.grantReadData(stockDailyGetFn);
    userDataTable.grantReadData(stockDailyGetFn);

    // -----------------------------------------------------------------------
    // 8. AI LAMBDAS
    // -----------------------------------------------------------------------
    const stockAiJobStarterFn = mkLambda(
      'StockAiJobStarter', 'stock-ai-job-starter', 'stockAiJobStarter', {
      environment: {
        AI_JOBS_TABLE: TABLE.AI_JOBS,
        // INSIGHT_SUGGESTIONS_FUNCTION_NAME added below
      }
    });
    aiJobsTable.grantReadWriteData(stockAiJobStarterFn);
    stockAiJobStarterFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:Query'],
      resources: [
        `arn:aws:dynamodb:us-east-1:${this.account}:table/ai-jobs/index/callerIp-createdAt-index`,
      ],
    }));

    const aiJobStatusCheckerFn = mkLambda(
      'AiJobStatusChecker', 'stock-ai-job-status-checker', 'aiJobStatusChecker', {
      environment: {
        AI_JOBS_TABLE: TABLE.AI_JOBS,
      }
    });
    aiJobsTable.grantReadData(aiJobStatusCheckerFn);

    const stockAiInsightSuggestionsFn = mkLambda(
      'StockAiInsightSuggestions', 'stock-ai-insight-suggestions', 'stockAiInsightSuggestions', {
      timeout: cdk.Duration.minutes(15),
      memorySize: 512,
      environment: {
        AI_JOBS_TABLE:      TABLE.AI_JOBS,
        USER_DATA_TABLE:    TABLE.USER,
        MARKET_CACHE_TABLE: TABLE.MARKET,
        DAILY_CACHE_TABLE:  TABLE.DAILY,
      }
    });
    aiJobsTable.grantReadWriteData(stockAiInsightSuggestionsFn);
    userDataTable.grantReadData(stockAiInsightSuggestionsFn);
    marketCacheTable.grantReadData(stockAiInsightSuggestionsFn);
    dailyCacheTable.grantReadData(stockAiInsightSuggestionsFn);
    stockAiInsightSuggestionsFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel', 'bedrock:InvokeModelWithResponseStream'],
      resources: [
        'arn:aws:bedrock:*::foundation-model/anthropic.claude*',
        'arn:aws:bedrock:*:*:inference-profile/us.anthropic.claude*',
      ],
    }));
    stockAiInsightSuggestionsFn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'aws-marketplace:ViewSubscriptions',
        'aws-marketplace:Subscribe',
        'aws-marketplace:Unsubscribe',
      ],
      resources: ['*'],
    }));
    // Add after existing stockAiInsightSuggestionsFn permissions
    stockAiInsightSuggestionsFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [
        stockIntradayListGetFn.functionArn,
        stockDailyGetFn.functionArn,
      ],
    }));
    stockAiInsightSuggestionsFn.addEnvironment(
      'INTRADAY_LIST_FUNCTION_NAME',
      stockIntradayListGetFn.functionName
    );
    stockAiInsightSuggestionsFn.addEnvironment(
      'DAILY_LIST_FUNCTION_NAME',
      stockDailyGetFn.functionName
    );

    stockAiJobStarterFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [stockAiInsightSuggestionsFn.functionArn],
    }));
    stockAiJobStarterFn.addEnvironment(
      'INSIGHT_SUGGESTIONS_FUNCTION_NAME',
      stockAiInsightSuggestionsFn.functionName
    );

    // -----------------------------------------------------------------------
    // 9. EVENTBRIDGE LAMBDAS
    // -----------------------------------------------------------------------
    const stocksEventBridgeFn = mkLambda(
      'StocksEventBridge', 'stocks-eventbridge', 'stocksEventBridge', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        POLYGON_API_KEY:    props.polygonApiKey ?? '',
        MARKET_CACHE_TABLE: TABLE.MARKET,
        USER_DATA_TABLE:    TABLE.USER,
      }
    });
    marketCacheTable.grantReadWriteData(stocksEventBridgeFn);
    userDataTable.grantReadData(stocksEventBridgeFn);

    const stockDailyEventBridgeFn = mkLambda(
      'StockDailyEventBridge', 'stock-daily-event-bridge', 'stockDailyEventBridge', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        POLYGON_API_KEY:   props.polygonApiKey ?? '',
        DAILY_CACHE_TABLE: TABLE.DAILY,
      }
    });
    dailyCacheTable.grantReadWriteData(stockDailyEventBridgeFn);

    const stockPortfolioSnapshotsFn = mkLambda(
      'StockPortfolioSnapshots', 'stock-portfolio-snapshots', 'stockPortfolioSnapshots', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        USER_DATA_TABLE:    TABLE.USER,
        MARKET_CACHE_TABLE: TABLE.MARKET,
        DAILY_CACHE_TABLE:  TABLE.DAILY,
      }
    });
    userDataTable.grantReadWriteData(stockPortfolioSnapshotsFn);
    marketCacheTable.grantReadData(stockPortfolioSnapshotsFn);
    dailyCacheTable.grantReadData(stockPortfolioSnapshotsFn);

    const stockIntradayDeleteFn = mkLambda(
      'StockIntradayDelete', 'stock-intraday-delete', 'stockIntradayDelete', {
      timeout: cdk.Duration.minutes(5),
      environment: {
        MARKET_CACHE_TABLE: TABLE.MARKET,
      }
    });
    marketCacheTable.grantReadWriteData(stockIntradayDeleteFn);

    // -----------------------------------------------------------------------
    // 10. EVENTBRIDGE SCHEDULES & RULES
    // -----------------------------------------------------------------------

    // ── EventBridge Rule: intraday-delete ─────────────────────────────────────
    // cron(0 13 ? * MON-FRI *) UTC
    // If this rule already exists in your account, comment out the block below
    // and instead run this CLI command after deploy to retarget it:
    //
    // aws events put-targets --rule intraday-delete \
    //   --targets '[{"Id":"1","Arn":"'"$(aws lambda get-function --function-name stock-intraday-delete-v2 --query 'Configuration.FunctionArn' --output text)"'"}]'
    //

    // ── EventBridge Schedulers (existing — managed outside CDK) ──────────────
    // These three schedules already exist in the account and cannot be created
    // by CDK without conflict. After deploying this stack, update each schedule
    // in the AWS Console (or via CLI below) to point to the new -v2 Lambda ARNs.
    //
    // Schedule: Stock-Periodic          → target: stocks-eventbridge-v2
    // Schedule: stock-daily-eventbridge → target: stock-daily-event-bridge-v2
    // Schedule: Stock-Portfolio-Snapshots → target: stock-portfolio-snapshots-v2
    //
    // CLI commands to retarget (run after cdk deploy):
    //
    // aws scheduler update-schedule \
    //   --name Stock-Periodic \
    //   --schedule-expression "rate(5 minutes)" \
    //   --flexible-time-window '{"Mode":"OFF"}' \
    //   --target '{"Arn":"'"$(aws lambda get-function --function-name stocks-eventbridge-v2 --query 'Configuration.FunctionArn' --output text)"'","RoleArn":"SCHEDULER_ROLE_ARN"}'
    //
    // aws scheduler update-schedule \
    //   --name stock-daily-eventbridge \
    //   --schedule-expression "cron(51 16 ? * MON-FRI *)" \
    //   --schedule-expression-timezone "America/New_York" \
    //   --flexible-time-window '{"Mode":"OFF"}' \
    //   --target '{"Arn":"'"$(aws lambda get-function --function-name stock-daily-event-bridge-v2 --query 'Configuration.FunctionArn' --output text)"'","RoleArn":"SCHEDULER_ROLE_ARN"}'
    //
    // aws scheduler update-schedule \
    //   --name Stock-Portfolio-Snapshots \
    //   --schedule-expression "cron(50 16 ? * MON-FRI *)" \
    //   --schedule-expression-timezone "America/New_York" \
    //   --flexible-time-window '{"Mode":"FLEXIBLE","MaximumWindowInMinutes":5}' \
    //   --target '{"Arn":"'"$(aws lambda get-function --function-name stock-portfolio-snapshots-v2 --query 'Configuration.FunctionArn' --output text)"'","RoleArn":"SCHEDULER_ROLE_ARN"}'
    //
    // Replace SCHEDULER_ROLE_ARN with the existing role ARN from the schedule details.

    // -----------------------------------------------------------------------
    // 11. API GATEWAY
    // -----------------------------------------------------------------------
    const api = new apigateway.RestApi(this, 'StockAppApi', {
      restApiName: 'stock-app-api',
      description: 'Stock App REST API with Cognito auth',
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ['Content-Type', 'Authorization'],
      },
      deployOptions: {
        stageName: 'prod',
        methodOptions: {
          '/ai-insight/POST': {
            throttlingRateLimit: 1,
            throttlingBurstLimit: 1,
          },
        },
      },
    });


    const securityHeaders = {
      'Strict-Transport-Security': "'max-age=31536000; includeSubdomains; preload'",
      'X-Content-Type-Options':    "'nosniff'",
      'X-Frame-Options':           "'DENY'",
      'X-XSS-Protection':          "'1; mode=block'",
      'Referrer-Policy':           "'strict-origin-when-cross-origin'",
    };
    const gatewayCorsHeaders = {
      'Access-Control-Allow-Origin':  "'*'",
      'Access-Control-Allow-Headers': "'Content-Type,Authorization'",
      'Access-Control-Allow-Methods': "'OPTIONS,GET,POST,PUT,DELETE'",
    };

    new apigateway.GatewayResponse(this, 'GatewayResponse4xx', {
      restApi: api,
      type: apigateway.ResponseType.DEFAULT_4XX,
      responseHeaders: { ...securityHeaders, ...gatewayCorsHeaders },
    });

    new apigateway.GatewayResponse(this, 'GatewayResponse5xx', {
      restApi: api,
      type: apigateway.ResponseType.DEFAULT_5XX,
      responseHeaders: { ...securityHeaders, ...gatewayCorsHeaders },
    });

    // ── Cognito Authorizer ───────────────────────────────────────────────────────
    const userPool = cognito.UserPool.fromUserPoolId(
      this,
      'ImportedUserPool',
      'us-east-1_9VbCwStHJ',
    );

    const cognitoAuthorizer = new apigateway.CognitoUserPoolsAuthorizer(this, 'CognitoAuthorizerV4', {
      cognitoUserPools: [userPool],
      identitySource: 'method.request.header.Authorization',
    });

    const cfnAuthorizer = cognitoAuthorizer.node.defaultChild as apigateway.CfnAuthorizer;
    cfnAuthorizer.authorizerResultTtlInSeconds = 300;
    //cfnAuthorizer.identityValidationExpression = '^Bearer [-0-9a-zA-Z._]*$';

    const auth = {
      authorizer: cognitoAuthorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    };

    // ── Usage Plan (rate limiting) ───────────────────────────────────────────────
    const usagePlan = api.addUsagePlan('PublicUsagePlan', {
      name: 'PublicRateLimit',
      throttle: {
        rateLimit: 1,    // 5 requests per second steady state
        burstLimit: 2,   // no burst headroom — hard ceiling at 5
      },
      quota: {
        limit: 50,
        period: apigateway.Period.DAY,
      },
    });

    usagePlan.addApiStage({
      stage: api.deploymentStage,
    });


    const int = (fn: lambda.Function) => new apigateway.LambdaIntegration(fn);

    // ── /user ── ALL require auth ─────────────────────────────────────────────────
    const userRes = api.root.addResource('user');
    userRes.addMethod('GET', int(stockUsersGetFn), auth);
    userRes.addMethod('PUT', int(stockUsersPutFn), auth);
    userRes.addResource('watchlist').addMethod('DELETE', int(stockWatchlistDeleteFn), auth);
    userRes.addResource('portfolio-reset').addMethod('DELETE', int(stockPortfolioResetFn), auth);
    userRes.addResource('portfolio-analytics').addMethod('GET', int(stockPortfolioAnalyticsFn), auth);

    // ── /name ── requires auth ────────────────────────────────────────────────────
    const nameRes = api.root.addResource('name');
    nameRes.addMethod('GET', int(stockUsersGetFn), auth);
    nameRes.addMethod('PUT', int(stockUsersPutFn), auth);

    // ── /intraday ── public (market data) ─────────────────────────────────────────
    const intradayRes = api.root.addResource('intraday');
    intradayRes.addMethod('GET', int(stockLatestPriceGetFn));
    intradayRes.addMethod('POST', int(stockIntradayPutFn));
    intradayRes.addResource('holdings-prices').addMethod('POST', int(stockHoldingsChangeGetFn));
    intradayRes.addResource('latest').addMethod('GET', int(stockLatestPriceGetFn));
    intradayRes.addResource('request').addMethod('POST', int(stockIntradayPutFn));
    intradayRes.addResource('sparkline-market').addMethod('GET', int(stockIntradaySparklineFn));
    intradayRes.addResource('list').addMethod('POST', int(stockIntradayListGetFn));

    // ── /movers ── public ─────────────────────────────────────────────────────────
    const moversRes = api.root.addResource('movers');
    moversRes.addMethod('GET', int(stockMoversGetFn));
    moversRes.addMethod('POST', int(stockMoversPutFn));

    // ── /news ── public ───────────────────────────────────────────────────────────
    const newsRes = api.root.addResource('news');
    newsRes.addMethod('GET', int(stockNewsGetFn));

    // ── /daily ── public ──────────────────────────────────────────────────────────
    const dailyRes = api.root.addResource('daily');
    dailyRes.addMethod('GET', int(stockDailyGetFn));
    dailyRes.addMethod('POST', int(stockDailyPutFn));
    dailyRes.addResource('list').addMethod('POST', int(stockDailyGetFn));

    // ── /ai-insight ── POST/GET public (freemium), portfolio-summary requires auth ─
    const aiRes = api.root.addResource('ai-insight');
    aiRes.addMethod('GET', int(aiJobStatusCheckerFn));
    aiRes.addMethod('POST', int(stockAiJobStarterFn));
    aiRes.addResource('{jobId}').addMethod('GET', int(aiJobStatusCheckerFn));
    aiRes.addResource('portfolio-summary').addMethod('GET', int(stockAiInsightSuggestionsFn), auth);


    // -----------------------------------------------------------------------
    // 12. OUTPUTS
    // -----------------------------------------------------------------------
    new cdk.CfnOutput(this, 'ApiEndpoint', {
      value: api.url,
      description: 'API Gateway endpoint URL',
    });
  }
}