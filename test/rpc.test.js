'use strict';

const expect = require('expect');
const assert = require('assert');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('rpc', () => {

    let runFiboRpc;
    let client;
    let server;

    before(() => {
        client = queueTransport({ url: rabbitUrl });
        server = queueTransport({ url: rabbitUrl });

        runFiboRpc = client.rpc({
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
            },
        });

        server.server({
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

        return Promise.all([
            client.getReady(),
            server.getReady()
        ]);
    });

    after(() => Promise.all([client.close(), server.close()]));

    it('should work', () => {
        return runFiboRpc({ n: 1 });
    });

    it('should catch', () => {
        return runFiboRpc({ n: -1 })
            .then(() => {
                throw new Error('Unexpected success');
            })
            .catch(err => {
                expect(err.message).toEqual('n is not a natural number');
            });
    });

});

function calculateNonRecursive(n) {
    assert(isNatural(n), 'n is not a natural number');

    let a = 1, b = 1, next;
    if (n <= 2) {
        return 1;
    }

    for (let i = 2; i < n; i += 1) {
        next = a + b;
        a = b;
        b = next;
    }

    return next;
}

function isNatural(n) {
    return 'number' === typeof n && n >= 1 && n === Math.round(n);
}

