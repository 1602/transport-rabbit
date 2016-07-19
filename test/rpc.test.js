'use strict';

const expect = require('expect');
const assert = require('assert');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('rpc', () => {

    context('normal flow', () => {

        let transport = null;
        let runFiboRpc = null;

        beforeEach(() => {
            transport = createTransport({
                url: rabbitUrl
            });

            runFiboRpc = transport.rpcClient('fibonacci');

            transport.rpcServer('fibonacci', {
                handler(payload, job) {
                    job.ack();
                    return calculateNonRecursive(payload.n);
                }
            });

            return transport.getReady();
        });

        afterEach(() => transport.close());

        it('should work', () => {
            return runFiboRpc({ n: 5 })
                .then(res => expect(res).toEqual(5));
        });

        it('should end by timeout', () => {
            return runFiboRpc({ n: 500 }, {
                timeout: 100
            })
                .then(() => {
                    throw new Error('Unexpected success');
                }, err => expect(err.message).toExist());
        });

        it('should catch', () => {
            return runFiboRpc({ n: -1 })
                .then(() => {
                    throw new Error('Unexpected success');
                }, err => expect(err.message).toExist());
        });

    });

    context('server sends nack', () => {

        let transport = null;
        let runFiboRpc = null;
        let requeued = false;

        beforeEach(() => {
            transport = createTransport({
                url: rabbitUrl
            });

            runFiboRpc = transport.rpcClient('fibonacci');

            requeued = false;

            transport.rpcServer('fibonacci', {
                handler(payload, job) {
                    if (!requeued) {
                        requeued = true;
                        return job.nack();
                    }
                    return calculateNonRecursive(payload.n);
                }
            });

            return transport.getReady();
        });

        afterEach(() => transport.close());

        it('should requeue message', () => {
            return runFiboRpc({ n: 8 })
                .then(res => {
                    expect(requeued).toEqual(true);
                    expect(res).toEqual(21);
                });
        });

    });

});

function calculateNonRecursive(n) {
    assert(isNatural(n), 'n is not a natural number');

    if (n >= 500) {
        return new Promise(resolve => setTimeout(() => resolve(100000), 300));
    }

    if (n <= 2) {
        return 1;
    }

    let a = 1, b = 1, next;

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

