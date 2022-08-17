const kafkaClient = require('./kafkaClient')

const order = {
  address: 'some address',
  price: 300,
  items: ['phone', 'monitor']
}

async function main() {
  const producer = kafkaClient.producer()

  await producer.connect()
  await producer.send({ topic: 'order', messages: [{ value: JSON.stringify(order) }] })

  await producer.disconnect()
}

main()