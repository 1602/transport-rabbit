'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */
describe('pubsub', () => {

    let transport;
    let produce;
    const results1 = [];
    const results2 = [];

    before(() => {
        transport = queueTransport({ url: rabbitUrl });

        produce = transport.broadcaster('broadcast-fanout');

        transport.receiver('broadcast-fanout', res => results1.push(res));
        transport.receiver('broadcast-fanout', res => results2.push(res));

        return transport.getReady();
    });

    after(() => transport.close());

    it('should receive message to both queues', () => {
        produce('message');
        return new Promise(r => setTimeout(r, 300))
            .then(() => {
                expect(results1[0]).toBe('message');
                expect(results2[0]).toBe('message');
            });
    });

});

