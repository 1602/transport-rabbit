'use strict';

const queueTransport = require('transport-rabbit');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    prefetch: 1,
    quitGracefullyOnTerm: true
});

const resolveCommand = transport.client({
    produce: {
        queue: {
            routes: [ 'command' ],
            exchange: 'url-resolution',
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    }
});

transport.server({
    consume: {
        queue: {
            routes: [ 'error', 'result' ],
            exchange: 'url-resolution',
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    },
    handler: {
        result: opts => console.log('received result', opts),
        error: err => console.log('received error', err)
    }
});

const int = setInterval(() => {
    if (transport.isConnected()) {
        resolveCommand({
            url: 'http://google.com',
            proxy: 'gb'
        });
        setTimeout(() => clearInterval(int), 5000);
    }
}, 1000);

