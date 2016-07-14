'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('consumer', () => {

    let transport = null;

    beforeEach(() => transport = queueTransport({
        url: rabbitUrl
    }));

    afterEach(() => transport.close());

    it('should consume queues', () => {
        transport.consumer({
            channelName: 'custom',
            exchangeName: 'task',
            queueName: 'command'
        });

        let queueConsumed = false;
        const chan = transport.getChannel('custom');
        chan.consume = () => {
            queueConsumed = true;
            return Promise.resolve();
        };

        return transport.getReady()
            .then(() => {
                expect(queueConsumed).toBe(true);
            });
    });

});

