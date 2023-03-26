import { Kafka, Partitioners } from "kafkajs";
import { v4 as UUID } from "uuid";
console.log("*** Producer starts... ***");

const kafka = new Kafka({
    clientId: 'checker-server',
    brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const consumer = kafka.consumer({ groupId: 'kafka-checker-servers2' });

const run = async () => {

    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({ topic: 'checkedresult' })

    setInterval(() => {
        queueMessage();
    }, 2500)

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString()
            })
        }
    })
}

run().catch(console.error);

const idNumbers = [
  'NNN588+9999',
  '112233-9999',
  '300233-9999',
  '30233-9999',
  '171232B9330',
  '010105A983E',
  '171232A9330',
  '180408A920K',
  '190301A990V',
  '050262+9449',
];

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from + 1))) + from;
}

async function queueMessage() {

    const uuidFraction = UUID().substring(0, 4);

    const success = await producer.send({
        topic: 'tobechecked',
        messages: [
            {
                key: uuidFraction,
                value: Buffer.from(idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]),
                valid: Boolean,
            },
        ],
    });

    if (success) {
        console.log(`Message ${uuidFraction} succesfully to the stream`);
    } else {
        console.log('Problem writing to stream');
    }
}