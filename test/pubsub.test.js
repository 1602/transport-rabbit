'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */
describe.skip('pubsub', function() {

    let transport;

    beforeEach(function() {
        transport = queueTransport({ url: rabbitUrl });
    });

    afterEach(function() {
        return transport.close();
    });

    it('should receive message to both queues', function() {
        const publish = transport.publisher('pubsub.test');

        const results1 = [];
        const results2 = [];

        transport.subscriber('pubsub.test', {
            topic: 'something',
            consume: res => results1.push(res)
        });

        transport.subscriber('pubsub.test', {
            topic: 'something',
            consume: res => results2.push(res)
        });

        return transport.connect()
            .then(() => publish('message', 'something'))
            .then(() => new Promise(resolve => setTimeout(resolve, 300)))
            .then(() => {
                expect(results1[0]).toBe('message');
                expect(results2[0]).toBe('message');
            });
    });

});

