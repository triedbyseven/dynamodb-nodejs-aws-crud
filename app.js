const express = require('express');
const app = express();
const { dynamodb, docClient } = require('./dynamodb');

const TABLE_NAME = 'CalendarEventApp';
// const TABLE_NAME = 'OnlineShop';
// const TABLE_NAME = 'OnlineShop1';

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
      },
      {
        AttributeName: 'GSI1-PK',
        AttributeType: 'S'
      },
      {
        AttributeName: 'GSI1-SK',
        AttributeType: 'S'
      },
      {
        AttributeName: 'GSI2-PK',
        AttributeType: 'S'
      },
      {
        AttributeName: 'GSI2-SK',
        AttributeType: 'S'
      },
    ],
    GlobalSecondaryIndexes: [
      {
        IndexName: 'GSI1', /* required */
        KeySchema: [ /* required */
          {
            AttributeName: 'GSI1-PK', /* required */
            KeyType: 'HASH' /* required */
          },
          {
            AttributeName: 'GSI1-SK', /* required */
            KeyType: 'RANGE' /* required */
          },
          /* more items */
        ],
        Projection: { /* required */
          NonKeyAttributes: [
            // 'STRING_VALUE',
            /* more items */
          ],
          ProjectionType: 'ALL'
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 1, /* required */
          WriteCapacityUnits: 1 /* required */
        }
      },
      {
        IndexName: 'GSI2', /* required */
        KeySchema: [ /* required */
          {
            AttributeName: 'GSI2-PK', /* required */
            KeyType: 'HASH' /* required */
          },
          {
            AttributeName: 'GSI2-SK', /* required */
            KeyType: 'RANGE' /* required */
          },
          /* more items */
        ],
        Projection: { /* required */
          NonKeyAttributes: [
            // 'STRING_VALUE',
            /* more items */
          ],
          ProjectionType: 'ALL'
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 1, /* required */
          WriteCapacityUnits: 1 /* required */
        }
      },
      /* more items */
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

app.get('/createCustomerToProduct', (request, response) => {
  var params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': 'p#12345',
      'SK': 'c#12345',
      'EntityType': 'customer'
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
})

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
      'SK': 'w#12344',
      'EntityType': 'warehouseItem',
      'Quantity': '150',
      'GSI1-PK': 'p#12345',
      'GSI1-SK': '2022-09-08T23:52:31+00:00'
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

app.get('/getProductDetailed', (request, response) => {
  const params = {
    TableName: TABLE_NAME,
    ScanIndexForward: true,
    ConsistentRead: false,
    KeyConditionExpression: "#pk = :pk",
    ExpressionAttributeNames: {
      "#pk": "PK",
    },
    ExpressionAttributeValues: {
      ":pk": "p#12345",
    },
    ProjectionExpression: "EntityType, Detail, Quantity"
  };

  // With this approach. In order to set the graphQL return object with the complete customer object.
  // We would need to add the `SK` to the ProjectionExpression.
  // Then run a getItem request on that customer SK / ID.
  docClient.query(params, function (err, data) {
    if (err) return console.log("Error", err);

    const customerDocument = data.Items.find(document => document.EntityType === 'customer');
    const allWarehouseItemDocument = data.Items.filter(document => document.EntityType === 'warehouseItem');
    const productDocument = data.Items.find(document => document.EntityType === 'product');

    // GraphQL return object
    const productData = {
      product: {
        ...productDocument,
        customer: customerDocument,
        warehouseItems: allWarehouseItemDocument
      }
    };

    console.log('Product: ', productData);

    response.json({ data: productData });
  });
});

app.get('/listWarehouseItemsDateRange', (request, response) => {
  const params =  {
    TableName: TABLE_NAME,
    ScanIndexForward: true,
    IndexName: "GSI1",
    KeyConditionExpression: "#c1490 = :c1490 And #c1491 BETWEEN :c1491 AND :c1492",
    ExpressionAttributeValues: {
      ":c1490": "p#12345",
      ":c1491": "2022-09-06T23:52:31+00:00",
      ":c1492": "2022-09-09T23:52:31+00:00"
    },
    ExpressionAttributeNames: {
      "#c1490": "GSI1-PK",
      "#c1491": "GSI1-SK"
    }
  };

  docClient.query(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log('Product: ', data);

    response.json({ data: data });
  });
});

app.get('/listAllCustomersByProduct', (request, response) => {
  const params = {
    TableName: TABLE_NAME,
    ScanIndexForward: true,
    IndexName: "GSI1",
    KeyConditionExpression: "#c1490 = :c1490 And #c1491 = :c1491",
    ExpressionAttributeValues: {
      ":c1490": "p#12345", 
      ":c1491": "c#12345"
    },
    ExpressionAttributeNames: {
      "#c1490": "GSI1-PK",
      "#c1491": "GSI1-SK",
      "#FullName": "Name"
    },
    ProjectionExpression: "EntityType, Email, # FullName"
  };

  docClient.query(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log('Product: ', data);

    response.json({ data: data });
  });
});

app.listen(3002);