'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('client', () => {

    let transport;
    let enqueueMessage;

    before(() => {
        transport = queueTransport({
            url: rabbitUrl
        });

        enqueueMessage = transport.client({
            produce: {
                queue: {
                    exchange: 'log',
                    routes: [ 'info', 'warn', 'error' ]
                }
            }
        });

        return transport.getReady()
            .then(() => transport.queue.purge('log.info'));
    });

    after(() => transport.close());

    it('should publish message to queue', () => {
        return Promise.resolve(enqueueMessage('Hello world', 'info'))
            .then(result => {
                expect(result).toBe(undefined);
            })
            .then(() => Promise.all([
                transport.queue.messageCount('log.info'),
                transport.queue.messageCount('log.warn'),
                transport.queue.messageCount('log.error'),
            ]))
            .then(messageCounts => {
                expect(messageCounts[0]).toEqual(1);
                expect(messageCounts[1]).toEqual(0);
                expect(messageCounts[2]).toEqual(0);
            });
    });

    it('should live more than 1 second', done => setTimeout(done, 1200));

});

