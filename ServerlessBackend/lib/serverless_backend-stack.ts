import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

export interface StockAppStackProps extends cdk.StackProps {
  polygonApiKey?: string;
}

export class StockAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: StockAppStackProps) {
    super(scope, id, props);

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

    // -----------------------------------------------------------------------
    // 2. S3 — Static hosting for React Frontend
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // 3. SHARED LAMBDA CONFIG
    // -----------------------------------------------------------------------
    const commonEnv = {
      POLYGON_API_KEY: props.polygonApiKey,
      MARKET_CACHE_TABLE: marketCacheTable.tableName,
      DAILY_CACHE_TABLE: dailyCacheTable.tableName,
      MOVERS_TABLE: moversTable.tableName,
      USER_DATA_TABLE: userDataTable.tableName,
      AI_JOBS_TABLE: aiJobsTable.tableName,
    };

    const commonProps = {
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      environment: commonEnv,
    };

    // Helper — creates a Lambda with consistent naming.
    // All function names get a -v2 suffix to avoid conflicts with the
    // existing manually-created Lambdas already deployed in this account.
    const mkLambda = (
      id: string,
      functionName: string,
      handlerDir: string,
      overrides: Partial<lambda.FunctionProps> = {}
    ) =>
      new lambda.Function(this, id, {
        ...commonProps,
        functionName: `${functionName}-v2`,
        handler: 'index.handler', // each lambda folder contains index.mjs
        code: lambda.Code.fromAsset(path.join(__dirname, `lambdas/${handlerDir}`)),
        ...overrides,
      } as lambda.FunctionProps);

    // -----------------------------------------------------------------------
    // 4. USER LAMBDAS
    //    GET    /users/get
    //    PUT    /user
    //    DELETE /user/watchlist
    //    DELETE /user/portfolio-reset
    // -----------------------------------------------------------------------
    const stockUsersGetFn = mkLambda(
      'StockUsersGet', 'stock-users-get', 'stockUsersGet'
    );
    userDataTable.grantReadData(stockUsersGetFn);

    const stockUsersPutFn = mkLambda(
      'StockUsersPut', 'stock-users-put', 'stockUsersPut'
    );
    userDataTable.grantReadWriteData(stockUsersPutFn);

    const stockWatchlistDeleteFn = mkLambda(
      'StockWatchlistDelete', 'stock-watchlist-delete', 'stockWatchlistDelete'
    );
    userDataTable.grantReadWriteData(stockWatchlistDeleteFn);

    const stockPortfolioResetFn = mkLambda(
      'StockPortfolioReset', 'stock-portfolio-reset', 'stockPortfolioReset'
    );
    userDataTable.grantReadWriteData(stockPortfolioResetFn);

    // -----------------------------------------------------------------------
    // 5. INTRADAY LAMBDAS
    //    POST /intraday/holdings/prices
    //    GET  /intraday/latest
    //    POST /intraday/request
    //    GET  /intraday/sparkline-market
    //    POST /intraday/list
    // -----------------------------------------------------------------------
    const stockHoldingsChangeGetFn = mkLambda(
      'StockHoldingsChangeGet', 'stock-holdings-change-get', 'stockHoldingsChangeGet'
    );
    marketCacheTable.grantReadData(stockHoldingsChangeGetFn);
    userDataTable.grantReadData(stockHoldingsChangeGetFn);

    // Declared before stockLatestPriceGetFn because latest-price-get invokes it on cache miss
    const stockIntradayPutFn = mkLambda(
      'StockIntradayPut', 'stock-intraday-put', 'stockIntradayPut',
      { timeout: cdk.Duration.minutes(5) }
    );
    marketCacheTable.grantReadWriteData(stockIntradayPutFn);

    const stockLatestPriceGetFn = mkLambda(
      'StockLatestPriceGet', 'stock-latest-price-get', 'stockLatestPriceGet'
    );
    marketCacheTable.grantReadData(stockLatestPriceGetFn);
    // stock-latest-price-get → stock-intraday-put (sync invoke on cache miss)
    stockLatestPriceGetFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [stockIntradayPutFn.functionArn],
    }));
    stockLatestPriceGetFn.addEnvironment(
      'INTRADAY_PUT_FUNCTION_NAME',
      stockIntradayPutFn.functionName
    );

    const stockIntradaySparklineFn = mkLambda(
      'StockIntradaySparkline', 'stock-intraday-sparkline', 'stockIntradaySparkline'
    );
    marketCacheTable.grantReadData(stockIntradaySparklineFn);

    const stockIntradayListGetFn = mkLambda(
      'StockIntradayListGet', 'stock-intraday-list-get', 'stockIntradayListGet'
    );
    marketCacheTable.grantReadData(stockIntradayListGetFn);
    userDataTable.grantReadData(stockIntradayListGetFn);

    // -----------------------------------------------------------------------
    // 6. MOVERS LAMBDAS
    //    GET  /movers
    //    POST /movers
    // -----------------------------------------------------------------------
    const stockMoversGetFn = mkLambda(
      'StockMoversGet', 'stock-movers-get', 'stockMoversGet'
    );
    moversTable.grantReadData(stockMoversGetFn);

    const stockMoversPutFn = mkLambda(
      'StockMoversPut', 'stock-movers-put', 'stockMoversPut',
      { timeout: cdk.Duration.minutes(5) }
    );
    moversTable.grantReadWriteData(stockMoversPutFn);

    // -----------------------------------------------------------------------
    // 7. DAILY LAMBDAS
    //    POST /daily
    //    POST /daily/list
    // -----------------------------------------------------------------------
    const stockDailyPutFn = mkLambda(
      'StockDailyPut', 'stock-daily-put', 'stockDailyPut',
      { timeout: cdk.Duration.minutes(5) }
    );
    dailyCacheTable.grantReadWriteData(stockDailyPutFn);

    const stockDailyGetFn = mkLambda(
      'StockDailyGet', 'stock-daily-get', 'stockDailyGet'
    );
    dailyCacheTable.grantReadData(stockDailyGetFn);
    userDataTable.grantReadData(stockDailyGetFn);

    // -----------------------------------------------------------------------
    // 8. AI LAMBDAS
    //    POST /ai              → stock-ai-job-starter
    //    GET  /ai/{jobId}      → stock-ai-job-status-checker
    //    (internal async)      → stock-ai-insight-suggestions  (Bedrock invoker)
    // -----------------------------------------------------------------------
    const stockAiJobStarterFn = mkLambda(
      'StockAiJobStarter', 'stock-ai-job-starter', 'stockAiJobStarter'
    );
    aiJobsTable.grantReadWriteData(stockAiJobStarterFn);

    const aiJobStatusCheckerFn = mkLambda(
      'AiJobStatusChecker', 'stock-ai-job-status-checker', 'aiJobStatusChecker'
    );
    aiJobsTable.grantReadData(aiJobStatusCheckerFn);

    // Long-running Bedrock invoker — called async by job starter
    const stockAiInsightSuggestionsFn = mkLambda(
      'StockAiInsightSuggestions', 'stock-ai-insight-suggestions', 'stockAiInsightSuggestions',
      { timeout: cdk.Duration.minutes(15), memorySize: 512 }
    );
    aiJobsTable.grantReadWriteData(stockAiInsightSuggestionsFn);
    userDataTable.grantReadData(stockAiInsightSuggestionsFn);
    marketCacheTable.grantReadData(stockAiInsightSuggestionsFn);
    dailyCacheTable.grantReadData(stockAiInsightSuggestionsFn);

    // Bedrock model access
    stockAiInsightSuggestionsFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel', 'bedrock:InvokeModelWithResponseStream'],
      resources: ['arn:aws:bedrock:*::foundation-model/anthropic.claude-3-5-sonnet*'],
    }));

    // Allow job starter to fire insight suggestions asynchronously (Event invocation)
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
    //
    //  Schedule type      Name                      Lambda target
    //  ─────────────────  ────────────────────────  ──────────────────────────
    //  EB Rule  (cron)    intraday-delete           stock-intraday-delete
    //                     cron(0 13 ? * MON-FRI *)  UTC — runs at 13:00 UTC weekdays
    //
    //  EB Scheduler       Stock-Periodic            stocks-eventbridge
    //  (fixed rate)       rate(5 minutes)           America/New_York, no market-hour filter
    //
    //  EB Scheduler       stock-daily-eventbridge   stock-daily-event-bridge
    //  (cron)             cron(51 16 ? * MON-FRI *) America/New_York → 16:51 ET weekdays
    //
    //  EB Scheduler       Stock-Portfolio-Snapshots stock-portfolio-snapshots
    //  (cron)             cron(50 16 ? * MON-FRI *) America/New_York → 16:50 ET weekdays
    //                     Flexible time window: 5 minutes
    // -----------------------------------------------------------------------

    // Lambda: stocks-eventbridge (= Stock-Periodic target)
    const stocksEventBridgeFn = mkLambda(
      'StocksEventBridge', 'stocks-eventbridge', 'stocksEventBridge',
      { timeout: cdk.Duration.minutes(5) }
    );
    marketCacheTable.grantReadWriteData(stocksEventBridgeFn);
    userDataTable.grantReadData(stocksEventBridgeFn);

    // Lambda: stock-daily-event-bridge
    const stockDailyEventBridgeFn = mkLambda(
      'StockDailyEventBridge', 'stock-daily-event-bridge', 'stockDailyEventBridge',
      { timeout: cdk.Duration.minutes(5) }
    );
    dailyCacheTable.grantReadWriteData(stockDailyEventBridgeFn);

    // Lambda: stock-portfolio-snapshots
    const stockPortfolioSnapshotsFn = mkLambda(
      'StockPortfolioSnapshots', 'stock-portfolio-snapshots', 'stockPortfolioSnapshots',
      { timeout: cdk.Duration.minutes(5) }
    );
    userDataTable.grantReadWriteData(stockPortfolioSnapshotsFn);
    marketCacheTable.grantReadData(stockPortfolioSnapshotsFn);
    dailyCacheTable.grantReadData(stockPortfolioSnapshotsFn);

    // Lambda: stock-intraday-delete
    const stockIntradayDeleteFn = mkLambda(
      'StockIntradayDelete', 'stock-intraday-delete', 'stockIntradayDelete',
      { timeout: cdk.Duration.minutes(5) }
    );
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
      deployOptions: { stageName: 'prod' },
    });


    const int = (fn: lambda.Function) => new apigateway.LambdaIntegration(fn);

    // ── /user ────────────────────────────────────────────────────────────────
    // GET    /user              → stock-users-get
    // PUT    /user              → stock-users-put
    // DELETE /user/watchlist    → stock-watchlist-delete
    // DELETE /user/portfolio-reset → stock-portfolio-reset
    const userRes = api.root.addResource('user');
    userRes.addMethod('GET', int(stockUsersGetFn));
    userRes.addMethod('PUT', int(stockUsersPutFn));
    userRes.addResource('watchlist').addMethod('DELETE', int(stockWatchlistDeleteFn));
    userRes.addResource('portfolio-reset').addMethod('DELETE', int(stockPortfolioResetFn));

    // ── /name ─────────────────────────────────────────────────────────────────
    // GET /name  (TODO: confirm lambda)
    // PUT /name  (TODO: confirm lambda)
    const nameRes = api.root.addResource('name');
    nameRes.addMethod('GET', int(stockUsersGetFn));
    nameRes.addMethod('PUT', int(stockUsersPutFn));

    // ── /intraday ─────────────────────────────────────────────────────────────
    // GET  /intraday                  → stock-latest-price-get  (TODO: confirm)
    // POST /intraday                  → stock-intraday-put      (TODO: confirm)
    // POST /intraday/holdings-prices  → stock-holdings-change-get
    // GET  /intraday/latest           → stock-latest-price-get
    // POST /intraday/request          → stock-intraday-put
    // GET  /intraday/sparkline-market → stock-intraday-sparkline
    // POST /intraday/list             → stock-intraday-list-get
    const intradayRes = api.root.addResource('intraday');
    intradayRes.addMethod('GET', int(stockLatestPriceGetFn));
    intradayRes.addMethod('POST', int(stockIntradayPutFn));
    intradayRes.addResource('holdings-prices').addMethod('POST', int(stockHoldingsChangeGetFn));
    intradayRes.addResource('latest').addMethod('GET', int(stockLatestPriceGetFn));
    intradayRes.addResource('request').addMethod('POST', int(stockIntradayPutFn));
    intradayRes.addResource('sparkline-market').addMethod('GET', int(stockIntradaySparklineFn));
    intradayRes.addResource('list').addMethod('POST', int(stockIntradayListGetFn));

    // ── /movers ───────────────────────────────────────────────────────────────
    // GET  /movers → stock-movers-get
    // POST /movers → stock-movers-put
    const moversRes = api.root.addResource('movers');
    moversRes.addMethod('GET', int(stockMoversGetFn));
    moversRes.addMethod('POST', int(stockMoversPutFn));

    // ── /daily ────────────────────────────────────────────────────────────────
    // GET  /daily       → stock-daily-get  (TODO: confirm)
    // POST /daily       → stock-daily-put
    // POST /daily/list  → stock-daily-get
    const dailyRes = api.root.addResource('daily');
    dailyRes.addMethod('GET', int(stockDailyGetFn));
    dailyRes.addMethod('POST', int(stockDailyPutFn));
    dailyRes.addResource('list').addMethod('POST', int(stockDailyGetFn));

    // ── /ai-insight ───────────────────────────────────────────────────────────
    // GET  /ai-insight                   → stock-ai-job-status-checker (TODO: confirm)
    // POST /ai-insight                   → stock-ai-job-starter
    // GET  /ai-insight/{jobId}           → stock-ai-job-status-checker
    // GET  /ai-insight/portfolio-summary → stock-ai-insight-suggestions (TODO: confirm)
    const aiRes = api.root.addResource('ai-insight');
    aiRes.addMethod('GET', int(aiJobStatusCheckerFn));
    aiRes.addMethod('POST', int(stockAiJobStarterFn));
    aiRes.addResource('{jobId}').addMethod('GET', int(aiJobStatusCheckerFn));
    aiRes.addResource('portfolio-summary').addMethod('GET', int(stockAiInsightSuggestionsFn));


    // -----------------------------------------------------------------------
    // 12. OUTPUTS
    // -----------------------------------------------------------------------
    new cdk.CfnOutput(this, 'ApiEndpoint', {
      value: api.url,
      description: 'API Gateway endpoint URL',
    });
  }
}