const chai = require('chai')
const sinon = require('sinon')

var expect = chai.expect

const dbOperations = require('../dynamoDbOperations')

describe("DDB Operations", ()=>{
    it("put items", () =>{
      const dbClient = {
        update: sinon.stub().returns({promise: () => Promise.resolve(true)})
      }
      dbOperations.defineDb(dbClient)
      dbOperations.putItem('testTable', "pk_val", "sk_val", {oi: "abc", ola: "def"} )
      expect(dbClient.update.getCall(0).args[0].UpdateExpression).to.eql('SET #k0 = :v0, #k1 = :v1, updatedAt = :updatedAt')
      expect(dbClient.update.getCall(0).args[0].ExpressionAttributeNames).to.eql({ '#k0': 'oi', '#k1': 'ola' })
      expect(dbClient.update.getCall(0).args[0].Key).to.eql({ PK: 'pk_val', SK: 'sk_val' })
    })  

    it("get items", async () =>{
      const dbClient = {
        query: sinon.stub().returns({promise: () => Promise.resolve({Items: []})})
      }
      dbOperations.defineDb(dbClient)
      await dbOperations.getItems('testTable', "pk_val", "sk_val", {oi: "abc", ola: "def"} )
      // expect(dbClient.query.getCall(0).args[0].ExpressionAttributeNames).to.eql('SET #k0 = :v0, #k1 = :v1, updatedAt = :updatedAt') 
      
      expect(dbClient.query.getCall(0).args[0].KeyConditionExpression).to.eql('#k0 = :v0, #k1 = :v1, #k2 = :v2, #k3 = :v3')
      // expect(dbClient.query.getCall(0).args[0].ExpressionAttributeNames).to.eql({ '#k0': 'oi', '#k1': 'ola' })
      // expect(dbClient.query.getCall(0).args[0].Key).to.eql({ PK: 'pk_val', SK: 'sk_val' })
    })  
    
})