
const AWSXRay = require('aws-xray-sdk');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const {DateTime} = require('luxon');
var dynamoDb = new AWS.DynamoDB.DocumentClient();

const formatAttributes = async (data, letter= 'k') =>
  Object.keys(data).reduce((ac, cur, index) =>{
      ac[`#${letter}${index}`] = cur
      return ac
  }, {})

const formatValues = async (filterObject, attributes, letter = 'k') =>
  Object.keys(attributes)?.reduce((ac, cur) => {
    let letter = /\w/.exec(cur)[0]
    ac[`:${letter}${cur.split(`#${letter}`)[1]}`] = filterObject[attributes[cur]]
    return ac
  }, {}) ?? {}

const formatExpression = attributes => 
  Object.keys(attributes).map(k => {
    let letter = /\w/.exec(k)[0]
    return `${k} = :${letter}${k.split(`#${letter}`)[1]}`
  }).join(' and ')

const formatParams = (tableName, indexName, keyValues, limit, lastKey, filterObject) => 
    Promise.all([
      formatAttributes(keyValues), 
      formatAttributes(filterObject, 'f')
    ]).then(([keyAttrs, filterAttrs]) => 
      formatValues({...keyValues, ...filterObject}, {...keyAttrs, ...filterAttrs})
      .then(ExpressionAttributeValues =>
        ({
          TableName: tableName,
          ...(indexName? {IndexName: indexName}: {}),
          ...(limit? {Limit: limit}: {}),
          ...(lastKey? {ExclusiveStartKey: lastKey}: {}),
          ExpressionAttributeNames: {...keyAttrs, ...filterAttrs},
          ExpressionAttributeValues,
          KeyConditionExpression: formatExpression(keyAttrs),
          ...(Object.keys(filterAttrs)?.length > 0 ? {FilterExpression: formatExpression(filterAttrs)}: {}),
        })
      )
    )

const formatValuesWithMultipleOptions = async (filterObject, attrs, letter = 'k') =>
  Object.keys(attrs)?.reduce((ac, cur) => {
    console.log(`formatValuesMultiple: ${cur}`, /\w/.exec(cur))
    let letter = /\w/.exec(cur)[0]
    filterObject[attrs[cur]].notIn?.forEach((filter, i) =>
      ac[`:${letter}${cur.split(`#${letter}`)[1]}x${i}`] = filter
    )
    return ac
  }, {}) ?? {}

//   PK = 1, 
//   SK = 2
// {
//   assetId:{
//     notIn: ["PETZ3", "BBAS3"]
//   }
// }

// {
//   #k1: "PK",
//   #k2: "SK"
// }{
//   :k1: 1,
//   :k2: 2
// }
// {
//   #f1: "assetId"
// }{
//   :f1.1: "PETZ3",
//   :f1.2: "BBAS3"
// }
const formatExpressionWithMultipleOptions = (attrNames, attrValues) => 
  Object.keys(attrNames).map(k => {
    console.log(`k:${k}`, /(?<=#\w)[0-9]+/.exec(k))
    let groupNumber = /(?<=#\w)[0-9]+/.exec(k)?.[0]
    const matchNumber = new RegExp(`:\\w${groupNumber}x*`)
    return Object.keys(attrValues)
      .filter(attrValue =>console.log(`match:`, matchNumber, "attr:" , attrValue) || matchNumber.test(attrValue) )
      .reduce((ac, cur) =>
        console.log(`ac:`, ac, "k:" , k) || [...ac, `${k} <> ${cur}`]
      , []).join(' and ') 
  }).join(' and ') 

const formatParamsWithFilter = (tableName, indexName, keyValues, limit, lastKey, filterObject) => 
  Promise.all([
    formatAttributes(keyValues), 
    formatAttributes(filterObject, 'f'),
  ]).then(([keyAttrNames, filterAttrNames]) =>
    Promise.all([
      formatValues(keyValues, keyAttrNames),
      formatValuesWithMultipleOptions(filterObject, filterAttrNames,  'f')
    ]).then(([keyAttrValues, filterAttrValues]) => ({
      TableName: tableName,
      ...(indexName? {IndexName: indexName}: {}),
      ...(limit? {Limit: limit}: {}),
      ...(lastKey? {ExclusiveStartKey: lastKey}: {}),
      ExpressionAttributeNames: {...keyAttrNames, ...(Object.keys(filterAttrValues)?.length > 0 ? filterAttrNames : {})},
      ExpressionAttributeValues: {...keyAttrValues, ...filterAttrValues},
      KeyConditionExpression: formatExpression(keyAttrNames),
      ...(Object.keys(filterAttrValues)?.length > 0 ? {FilterExpression: formatExpressionWithMultipleOptions(filterAttrNames, filterAttrValues)}: {})
    }))
  )

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
 * @param {*} filterObject 
 * @param {*} partitionKeyName 
 * @param {*} sortKeyName 
 * @returns Promise with the transaction result
 */
export function updateItem(tableName, partitionKeyValue, sortKeyValue, filterObject = {}, partitionKeyName = "PK", sortKeyName = "SK"){
    const nameAttributes = Object.keys(filterObject).reduce((ac, cur, index) =>{
        ac[`#k${index}`] = cur
        return ac
    }, {})
    const valueAttributes = formatValues(filterObject, nameAttributes)
    const keys = {}
    keys[partitionKeyName] = partitionKeyValue
    keys[sortKeyName] = sortKeyValue

    const params = {
        TableName: tableName,
        Key: keys,
        ExpressionAttributeNames: nameAttributes,
        ExpressionAttributeValues: {...valueAttributes,
            ':updatedAt': DateTime.utc().toISO()
        },
        UpdateExpression: 'SET ' + Object.keys(nameAttributes).map(k => `${k} = :k${k.split('#k')[1]}`).join(', ') + ', updatedAt = :updatedAt',
        ReturnValues: 'UPDATED_NEW',
    };
    console.log(`About to update item:`, params)
    return dynamoDb.update(params).promise().then(result => result?.Attributes ?? result)
}
export const putItem = (tableName, partitionKeyValue, sortKeyValue, filterObject = {}, partitionKeyName = "PK", sortKeyName = "SK") =>
  mergeKeysWithValues(partitionKeyValue, sortKeyValue, filterObject, partitionKeyName, sortKeyName)
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
  filterObject = {}, 
  partitionKeyName = "PK", sortKeyName = "SK"
) =>
  (Object.values(filterObject).some(filter => filter.notIn)
  ? formatParamsWithFilter(tableName, indexName, 
    {
      [partitionKeyName]: partitionKeyValue, 
      ...(sortKeyValue? {[sortKeyName]: sortKeyValue}: {})
    }, 
    limit,     
    lastKey,
    filterObject
  )
  : formatParams(tableName, indexName, 
    {
      [partitionKeyName]: partitionKeyValue, 
      ...(sortKeyValue? {[sortKeyName]: sortKeyValue}: {})
    }, 
    limit,     
    lastKey,
    filterObject
  ))
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