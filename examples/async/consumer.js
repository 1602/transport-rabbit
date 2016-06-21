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
            exchange: 'url-resolution',
            routes: [ 'command' ],
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    },
    handler: {
        command: opts => {
            console.log('got command', opts);
            return new Promise(r => setTimeout(() => r(opts), 500));
        }
    },
    produce: {
        queue: {
            exchange: 'url-resolution',
            // result and error in conjuncation with server + produce
            // gives ability to produce result of job to queues
            routes: [ 'result', 'error' ],
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    },
});

process.once('SIGTERM', transport.close);
process.once('SIGINT', transport.close);

