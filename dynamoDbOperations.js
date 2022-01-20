
const AWSXRay = require('aws-xray-sdk');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const {DateTime} = require('luxon');
var dynamoDb = new AWS.DynamoDB.DocumentClient();

const formatAttributes = async (data, letter= 'k') =>
  Object.keys(data).reduce((ac, cur, index) =>{
      ac[`#${letter}${index}`] = cur
      return ac
  }, {})

const formatValues = (objectValues, attributes, letter = 'k') =>
  Object.keys(attributes)?.reduce((ac, cur) => {
    let letter = /\w/.exec(cur)[0]
    ac[`:${letter}${cur.split(`#${letter}`)[1]}`] = objectValues[attributes[cur]]
    return ac
  }, {}) ?? {}

const formatKeyValuesExpression = attributes => 
  Object.keys(attributes).map(k => {
    let letter = /\w/.exec(k)[0]
    return `${k} = :${letter}${k.split(`#${letter}`)[1]}`
  }).join(' and ')

const formatParams = (tableName, indexName, keyValues, limit, lastKey, objectValues) => 
    Promise.all([
      formatAttributes(keyValues), 
      formatAttributes(objectValues, 'f')
    ]).then(([keyAttrs, valueAttrs]) => ({
        TableName: tableName,
        ...(indexName? {IndexName: indexName}: {}),
        ...(limit? {Limit: limit}: {}),
        ...(lastKey? {ExclusiveStartKey: lastKey}: {}),
        ExpressionAttributeNames: {...keyAttrs, ...valueAttrs},
        ExpressionAttributeValues: formatValues({...keyValues, ...objectValues}, {...keyAttrs, ...valueAttrs}),
        KeyConditionExpression: formatKeyValuesExpression(keyAttrs),
        ...(Object.keys(valueAttrs)?.length > 0 ? {FilterExpression: formatKeyValuesExpression(valueAttrs)}: {}),
      }))

const mergeKeysWithValues = async (partitionKeyValue, sortKeyValue, objectValues, partitionKeyName, sortKeyName) => {
  const objectKeysAndValues = objectValues
  objectKeysAndValues[partitionKeyName] = partitionKeyValue
  if(sortKeyValue)
    objectKeysAndValues[sortKeyName] = sortKeyValue
  return objectKeysAndValues
}

export function defineDb(DbClient){
  dynamoDb = DbClient
}

/**
 * Put Item (new/existing) into Dynamo DB table
 * @param {*} tableName 
 * @param {*} partitionKeyValue 
 * @param {*} sortKeyValue 
 * @param {*} objectValues 
 * @param {*} partitionKeyName 
 * @param {*} sortKeyName 
 * @returns Promise with the transaction result
 */
export function updateItem(tableName, partitionKeyValue, sortKeyValue, objectValues = {}, partitionKeyName = "PK", sortKeyName = "SK"){
    const attributes = Object.keys(objectValues).reduce((ac, cur, index) =>{
        ac[`#k${index}`] = cur
        return ac
    }, {})
    const values = formatValues(objectValues, attributes)
    const keys = {}
    keys[partitionKeyName] = partitionKeyValue
    keys[sortKeyName] = sortKeyValue

    const params = {
        TableName: tableName,
        Key: keys,
        ExpressionAttributeNames: attributes,
        ExpressionAttributeValues: {...values,
            ':updatedAt': DateTime.utc().toISO()
        },
        UpdateExpression: 'SET ' + Object.keys(attributes).map(k => `${k} = :k${k.split('#k')[1]}`).join(', ') + ', updatedAt = :updatedAt',
        ReturnValues: 'UPDATED_NEW',
    };
    console.log(`About to update item:`, params)
    return dynamoDb.update(params).promise().then(result => result?.Attributes ?? result)
}
export const putItem = (tableName, partitionKeyValue, sortKeyValue, objectValues = {}, partitionKeyName = "PK", sortKeyName = "SK") =>
  mergeKeysWithValues(partitionKeyValue, sortKeyValue, objectValues, partitionKeyName, sortKeyName)
  .then(objectKeysAndValues =>  {
    const params = {
        TableName: tableName,
        Item: {
          ...objectKeysAndValues, 
          'createdAt': DateTime.utc().toISO()
        }
    };
    console.log(`About to put item:`, params)
    return dynamoDb.put(params).promise()
  })

export const getItems = (tableName, indexName, 
  partitionKeyValue, sortKeyValue = null, 
  limit = null, 
  lastKey = null,
  objectValues = {}, 
  partitionKeyName = "PK", sortKeyName = "SK"
) =>
  formatParams(tableName, indexName, 
    {
      [partitionKeyName]: partitionKeyValue, 
      ...(sortKeyValue? {[sortKeyName]: sortKeyValue}: {})
    }, 
    limit,     
    lastKey,
    objectValues
  )
  .then(async params => {
    let items = [], data = {}
    do{
        console.log(`About to query items:`, params)
        data = await dynamoDb.query(params).promise()
        items = [...items, ...data.Items]
        console.log("Items loaded:" + data.Count)
        params.ExclusiveStartKey = data.LastEvaluatedKey 
    }while(data.LastEvaluatedKey && (!limit || (limit && items.length < limit)))
    return {items, lastKey: data.LastEvaluatedKey}
  })

  
export function deleteItem(tableName, partitionKeyValue, sortKeyValue, partitionKeyName = "PK", sortKeyName = "SK"){
  // const attributes = Object.keys(objectValues).reduce((ac, cur, index) =>{
  //     ac[`#k${index}`] = cur
  //     return ac
  // }, {})
  // const values = formatValues(objectValues, attributes)
  const keys = {}
  keys[partitionKeyName] = partitionKeyValue
  keys[sortKeyName] = sortKeyValue

  const params = {
      TableName: tableName,
      Key: keys,
      // ...(Object.keys(objectValues)?.length > 0 ? {
      //   ExpressionAttributeNames: attributes,
      //   ExpressionAttributeValues: {...values}
      // } : {}),
  };
  console.log(`About to delete item:`, params)
  return dynamoDb.delete(params).promise()
}