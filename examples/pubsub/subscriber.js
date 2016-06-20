'use strict';

const queueTransport = require('transport-rabbit');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    prefetch: 1
});

transport.server({
    consume: {
        queue: {
            exchange: 'log',
            exchangeType: 'direct',
            routes: [ 'warning' ],
            autogenerateQueues: true,
            options: {
                exclusive: true,
                durable: false,
                autoDelete: false
            }
        }
    },
    handler: {
        warning: opts => {
            console.log('got log.info', opts);
            return new Promise(r => setTimeout(() => r(opts), 500));
        }
    },
});

process.once('SIGTERM', transport.close);
process.once('SIGINT', transport.close);

