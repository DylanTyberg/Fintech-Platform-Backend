#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { StockAppStack } from '../lib/serverless_backend-stack';

const app = new cdk.App();
new StockAppStack(app, 'ServerlessBackendStack', {
  
});
