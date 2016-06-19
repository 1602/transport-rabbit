'use strict';

const queueTransport = require('./transport');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    prefetch: 1,
    quitGracefullyOnTerm: true
})
    .client({
        name: 'resolve',
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
    })
    .server({
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
        transport.send('resolve', 'command', {
            url: 'http://google.com',
            proxy: 'gb'
        });
        setTimeout(() => clearInterval(int), 5000);
    }
}, 1000);

