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
    const clientSettings = { url: rabbitUrl, rpcExpirationInterval: 100 };

    beforeEach(() => {
        client = queueTransport(clientSettings);
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

    afterEach(() => Promise.all([client.close(), server.close()]));

    it('should work', () => {
        delete clientSettings.rpcTimeout;
        return runFiboRpc({ n: 1 });
    });

    it('should end by timeout', () => {
        clientSettings.rpcTimeout = 100;
        return runFiboRpc({ n: 500 })
            .then(() => {
                throw new Error('Unexpected success');
            })
            .catch(err => {
                expect(err.message).toEqual('Awaiting response handler expired by timeout');
            });
    });

    it('should catch', () => {
        delete clientSettings.rpcTimeout;
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

    if (n >= 500) {
        return new Promise(resolve => setTimeout(resolve, 300));
    }

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

