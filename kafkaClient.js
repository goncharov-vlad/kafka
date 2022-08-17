const { Kafka } = require('kafkajs')

module.exports = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
})