import { Kafka, Partitioners } from "kafkajs";
console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: 'checker-server',
    brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const consumer = kafka.consumer({ groupId: 'kafka-checker-servers1' });

const hetuTest = (hetu) => {
    // hetun muodon tarkistus
    const char = /^[0-9]{6}[A+-][0-9]{3}[0-9A-Z]$/;
    if (!char.test(hetu)) {
        return false;
    }
    // merkkien erotus
    const day = hetu.slice(0, 2);
    const month = hetu.slice(2, 4);
    const year = hetu.slice(4, 6);
    const separator = hetu.at(6);
    const id = hetu.slice(7, 10);

    // vuosisadan erotus
    let century;
    if (separator === '+') {
        century = 18;
    } else if (separator === '-') {
        century = 19;
    } else if (separator === 'A') {
        century = 20;
    } else return false;

    // laske jakojäännös ja tarkistusmerkki
    const checksum = day + month + year + id;
    const modulo = parseInt(checksum) % 31;
    const checksumChars = '0123456789ABCDEFHJKLMNPRSTUVWXY';
    const valid = checksumChars.at(modulo);
    return (hetu.at(10) === valid);
}

const run = async () => {

    await consumer.connect()
    await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true })
    await producer.connect()

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {

            if (hetuTest(message.value.toString())) {
                await producer.send({
                  topic: 'checkedresult',
                  messages: [
                    {
                      value: String('Henkilötunnus on oikea'),
                    }
                  ]
                })
              } else {
                await producer.send({
                  topic: 'checkedresult',
                  messages: [
                    {
                      value: String('Henkilötunnus on virheellinen'),
                    }
                  ]
                })
              }
              
            console.log({
                key: message.key.toString(),
                partition: message.partition,
                offset: message.offset,
                value: message.value.toString(),
                valid: hetuTest(message.value.toString()),
            })
        },
    })
}

run().catch(console.error);