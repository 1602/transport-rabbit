'use strict';

const queueTransport = require('transport-rabbit');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    prefetch: 1,
    quitGracefullyOnTerm: true
});

const emitLog = transport.client({
    produce: {
        queue: {
            exchangeType: 'direct',
            exchange: 'log',
            options: {
                exclusive: false,
                durable: false,
                autoDelete: false
            }
        }
    }
});

const int = setInterval(() => {
    if (transport.isConnected()) {
        emitLog('hello world (warn)', 'warning');
        setTimeout(() => clearInterval(int), 5000);
    }
}, 1000);

