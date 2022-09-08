const express = require('express');
const app = express();
const { dynamodb, docClient } = require('./dynamodb');

const TABLE_NAME = 'OnlineShop';

app.get('/', function (req, res) {
  res.send('Hello World');
});

app.get('/createTable', (request, response) => {
  // Create the DynamoDB service object
  // HASH = Partition Key
  // RANGE = Sort Key
  const params = {
    TableName: TABLE_NAME,
    KeySchema: [
      {
        AttributeName: 'PK',
        KeyType: 'HASH'
      },
      {
        AttributeName: 'SK',
        KeyType: 'RANGE'
      }
    ],
    AttributeDefinitions: [
      {
        AttributeName: 'PK',
        AttributeType: 'S'
      },
      {
        AttributeName: 'SK',
        AttributeType: 'S'
      }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    },
    StreamSpecification: {
      StreamEnabled: false
    }
  };

  // Call DynamoDB to create the table
  dynamodb.createTable(params, function (err, data) {
    if (err) return console.log("Error", err);
      
    console.log("Table Created", data);
    response.json({
      tableCreated: data
    });
  });
});

app.get('/listTables', (request, response) => {
  // Call DynamoDB to retrieve the list of tables
  dynamodb.listTables({ Limit: 10 }, function (err, data) {
    if (err) return console.log("Error", err.code);
            
    console.log("Table names are ", data.TableNames);
    response.json({ tables: data.TableNames })
  });
});

app.get('/deleteTable', (request, response) => {
  const params = {
    TableName: TABLE_NAME
  };

  // Call DynamoDB to delete the specified table
  dynamodb.deleteTable(params, function (err, data) {
    if (err && err.code === 'ResourceNotFoundException') return console.log("Error: Table not found");

    if (err && err.code === 'ResourceInUseException') return console.log("Error: Table in use");

    console.log("Success", data);

    response.json({
      tableDeleted: data
    });
  });
});

app.get('/createCustomer', (request, response) => {
  var params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': 'c#12345',
      'SK': 'c#12345',
      'EntityType': 'customer',
      'Email': 'mikedev0431@gmail.com',
      'Name': 'Michael Camacho'
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

app.get('/createProduct', (request, response) => {
  var params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': 'p#12345',
      'SK': 'p#12345',
      'EntityType': 'product',
      'Detail': {
        Name: 'Ultra Boost',
        Description: 'Awesome shoes that make you run fast.'
      },
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

app.get('/createWarehouse', (request, response) => {
  var params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': 'w#12345',
      'SK': 'w#12345',
      'EntityType': 'warehouse',
      'Address': {
        AddressData: "Some address data."
      }
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

app.get('/createWarehouseItem', (request, response) => {
  var params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': 'p#12345',
      'SK': 'w#12345',
      'EntityType': 'warehouseItem',
      'Quantity': '50'
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

app.get('/getProduct', (request, response) => {
  const params = {
    TableName: TABLE_NAME,
    Key: { 'PK': 'p#12345', 'SK': 'p#12345' }
  };

  docClient.get(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data.Item);

    response.json({ data: 'OK' });
  });
});

app.get('/getWarehouseItem', (request, response) => {
  const params = {
    TableName: TABLE_NAME,
    Key: { 'PK': 'p#12345', 'SK': 'w#12345' }
  };

  docClient.get(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data.Item);

    response.json({ data: 'OK' });
  });
});

app.get('/getWarehouseItems', (request, response) => {
  const params = {
    TableName: TABLE_NAME,
    ScanIndexForward: true,
    ConsistentRead: false,
    KeyConditionExpression: "#pk = :pk And begins_with(#sk, :sk)",
    ExpressionAttributeNames: {
      "#pk": "PK",
      "#sk": "SK"
    },
    ExpressionAttributeValues: {
      ":pk": "p#12345",
      ":sk": "w#"
    }
  };

  docClient.query(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

app.listen(3002);