const kafkaClient = require('../kafkaClient.js')

async function main() {
  const consumer = kafkaClient.consumer({ groupId: 'notification' })
  await consumer.connect()
  await consumer.subscribe({ topic: 'notification' })

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString())

      await handleNotification(order)
    }
  })
}

async function handleNotification(order) {
  //Some notification logic here
  console.log('Notification handled', order)
}

main()