const express = require('express')
const cors = require("cors");

const { Kafka } = require('kafkajs')
const { Partitioners } = require('kafkajs')

const app = express()

var corsOptions = {
    origin: "http://localhost:8000"
};

app.use(cors(corsOptions));

// parse requests of content-type - application/json
app.use(express.json());  /* bodyParser.json() is deprecated */

// parse requests of content-type - application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: true }));   /* bodyParser.urlencoded() is deprecated */

// eurekaHelper.registerWithEureka('email-service', PORT);

const kafka = new Kafka({
    clientId: 'order_service_producer',
    brokers: ['pkc-56d1g.eastus.azure.confluent.cloud:9092'],
    ssl: true,
    logLevel: 2,
    sasl: {
      mechanism: 'plain',
      username: '6D22ZK3ORK2HKWWP',
      password: 'bO2G1UCwUdBfS6j/mhg3ioXCFmqm4F3ej/A2evpvJeyk7nT3O/yTH+G1C4HtLBye'
    }
})

const topic = "topic_order_service"

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const Produce = async (message) => {
	await producer.connect()
	
    try {
        const res = await producer.send({
            topic,
            messages: [
                {
                    key: "order_update",
                    value: Buffer.from(JSON.stringify(message)),
                },
            ],
        })

        // if the message is written successfully, log it and increment `i`
        console.log("produced: ", res)
    } catch (err) {
        console.error("could not write message " + err)
    }
}

app.post('/', async (req, res) => {
    console.log(req.body)
    const resposta = await Produce(req.body)
    res.send({ resposta });
  });

// set port, listen for requests
const PORT = process.env.PORT || 8000;

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}.`);
});