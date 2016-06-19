'use strict';

const queueTransport = require('./transport');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    quitGracefullyOnTerm: true,
    prefetch: 1
})

const calcFibo = transport.rpc({
    produce: {
        queue: {
            exchange: 'fibonacci',
            routes: [ 'query' ],
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    }
});

setTimeout(() => calcFibo({ n: 10 })
    .then(result => console.info('Received result', result))
    .catch(err => console.error('Received error', err))
, 3000);

