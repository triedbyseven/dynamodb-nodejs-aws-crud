const AWS = require('aws-sdk');
require('dotenv').config();

AWS.config.update({ region: process.env.AWS_DEFAULT_REGION });

// AWS.config.getCredentials(function (err) {
//   if (err) return console.log(err.stack);

//   console.log("Access key:", AWS.config.credentials.accessKeyId);
//   console.log("Region:", AWS.config.region);
// });

module.exports = {
  dynamodb: new AWS.DynamoDB({
    apiVersion: '2012-08-10',
    endpoint: 'http://localhost:8000'
  }),
  docClient: new AWS.DynamoDB.DocumentClient({ 
    apiVersion: '2012-08-10',
    endpoint: 'http://localhost:8000'
  })
};