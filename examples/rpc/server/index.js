'use strict';

const queueTransport = require('./transport');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';
const assert = require('assert');

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    quitGracefullyOnTerm: true,
    prefetch: 1
});

transport.server({
    consume: {
        queue: {
            exchange: 'fibonacci',
            routes: [ 'query' ],
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    },
    handler: {
        query: opts => {
            return Promise.resolve(opts.n)
                .then(n => calculateNonRecursive(n));
        }
    }
});

function calculateNonRecursive(n) {
    assert(isNatural(n), 'n is not a natural number');
    let a = 1, b = 1, next;
    if (n <= 2) {
        return 1;
    } else {
        for (let i = 2; i < n; i += 1) {
            next = a + b;
            a = b;
            b = next;
        }
        return next;
    }
}

function isNatural(n) {
    return 'number' === typeof n && n >= 1 || n === Math.round(n);
}

