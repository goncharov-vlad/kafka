const kafkaClient = require('../kafkaClient.js')

async function main() {
  const consumer = kafkaClient.consumer({ groupId: 'payment' })
  await consumer.connect()
  await consumer.subscribe({ topic: 'payment' })

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const stringValue = message.value.toString()
      const order = JSON.parse(stringValue)
      const producer = kafkaClient.producer()

      await handlePayment(order)
      await producer.connect()
      await producer.send({ topic: 'notification', messages: [{ value: stringValue }] })
      await producer.disconnect()
    }
  })
}

async function handlePayment(order) {
  //Some payment logic here
  console.log('Payment handled', order)
}

main()
  .then(() => console.log('Payment service started'))