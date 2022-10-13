const express = require('express');
const app = express();
const { dynamodb, docClient } = require('./dynamodb');
const { v4: uuid } = require('uuid');

const TABLE_NAME = 'CalendarEventApp';

/* 
  Sample DynamodDB CRUD operations.
  The getAllEventsByUser & getAllUsersByEvent methods will need logic to support
  more than 100 items. We should look into what the approach would be for
  server-side validation.
*/

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
      {
        AttributeName: 'GSI3-PK',
        AttributeType: 'S'
      },
      {
        AttributeName: 'GSI3-SK',
        AttributeType: 'S'
      }
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
      {
        IndexName: 'GSI3', /* required */
        KeySchema: [ /* required */
          {
            AttributeName: 'GSI3-PK', /* required */
            KeyType: 'HASH' /* required */
          },
          {
            AttributeName: 'GSI3-SK', /* required */
            KeyType: 'RANGE' /* required */
          }
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

// Single
app.get('/createUser', (request, response) => {
  const UID = uuid();

  const params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': `u#${UID}`,
      'SK': `u#${UID}`,
      'EntityType': 'user',
      'Name': 'Mike Chavez'
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

// Single
app.get('/createEvent', (request, response) => {
  const UID = uuid();

  const params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': `e#${UID}`,
      'SK': `e#${UID}`,
      'EntityType': 'event',
      'Name': 'TwitchCon2023'
    }
  };

  docClient.put(params, function (err, data) {
    if (err) return console.log("Error", err);

    console.log("Success", data);

    response.json({ data: 'OK' });
  });
});

// One to One
app.get('/createInviteeToEvent', async (request, response) => {
  const UID = uuid();

  const params = {
    TableName: TABLE_NAME,
    Item: {
      'PK': 'e#135304ec-5a8b-4381-a4a9-93bd829f1035',
      'SK': `i#${UID}`,
      'GSI1-PK': 'u#574d271c-62d1-4f92-bc31-3d95240539be',
      'GSI1-SK': 'u#574d271c-62d1-4f92-bc31-3d95240539be',
      'EntityType': 'invitee'
    }
  };

  await docClient.put(params).promise();

  response.json({ data: 'OK' });
});

// Many to Many
app.get('/getAllEventsByUser', async (request, response) => {
  let events = [];

  const params = {
    TableName: TABLE_NAME,
    ScanIndexForward: true,
    IndexName: "GSI1",
    KeyConditionExpression: "#c1490 = :c1490 And #c1491 = :c1491",
    ExpressionAttributeValues: {
      ":c1490": "u#37770a60-90fc-43bc-9570-474d1dd2b21a",
      ":c1491": "u#37770a60-90fc-43bc-9570-474d1dd2b21a"
    },
    ExpressionAttributeNames: {
      "#c1490": "GSI1-PK",
      "#c1491": "GSI1-SK",
      "#GSI1PK": "GSI1-PK",
      "#PK": "PK",
    },
    ProjectionExpression: "EntityType, #GSI1PK, #PK"
  };
  
  const data = await docClient.query(params).promise();
  events = data.Items;

  if (events.length === 0) return response.json({ data: [] });

  const keys = events.map((event) => {
    const the_user = {
      'PK': event['PK'],
      'SK': event['PK'],
    };

    return the_user;
  });

  const batchGetParams = {
    'RequestItems': {
      'CalendarEventApp': {
        'Keys': keys
      }
    }
  };

  const batchGetResponse = await docClient.batchGet(batchGetParams).promise();
  events = batchGetResponse.Responses.CalendarEventApp;

  console.log('All events by user: ', batchGetResponse.Responses.CalendarEventApp);

  response.json({ data: events });
});

// Many to Many
app.get('/getAllUsersByEvent', async (request, response) => {
  let users = [];

  const params = {
    TableName: TABLE_NAME,
    ScanIndexForward: true,
    ConsistentRead: false,
    KeyConditionExpression: "#pk = :pk And begins_with(#sk, :sk)",
    ExpressionAttributeNames: {
      "#pk": "PK",
      "#sk": "SK",
      "#GSI1": 'GSI1-PK'
    },
    ExpressionAttributeValues: {
      ":pk": "e#135304ec-5a8b-4381-a4a9-93bd829f1035",
      ":sk": "i#"
    },
    ProjectionExpression: "EntityType, #pk, #sk, #GSI1"
  };

  const data = await docClient.query(params).promise();
  users = data.Items;

  const keys = users.map((user) => {
    const the_event = {
      'PK': user['GSI1-PK'],
      'SK': user['GSI1-PK'],
    };

    return the_event;
  });

  const batchGetParams = {
    'RequestItems': {
      'CalendarEventApp': {
        'Keys': keys
      }
    }
  };

  const batchGetResponse = await docClient.batchGet(batchGetParams).promise();
  users = batchGetResponse.Responses.CalendarEventApp;

  console.log('All users by event: ', users);

  response.json({ data: users });
});

/* 
  Old DynamoDB CRUD operations.
*/

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