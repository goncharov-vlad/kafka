const kafkaClient = require('../kafkaClient.js')

async function main() {
  const consumer = kafkaClient.consumer({ groupId: 'order' })
  await consumer.connect()
  await consumer.subscribe({ topic: 'order' })

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const stringValue = message.value.toString()
      const order = JSON.parse(stringValue)
      const producer = kafkaClient.producer()

      await handleOrder(order)
      await producer.connect()
      await producer.send({ topic: 'payment', messages: [{ value: stringValue }] })
      await producer.disconnect()
    }
  })
}

async function handleOrder(order) {
  //Some order logic here
  console.log('Order handled', order)
}

main()
  .then(() => console.log('Order service started'))