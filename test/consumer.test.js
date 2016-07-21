'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe.skip('consumer', () => {

    let transport = null;

    beforeEach(() => transport = createTransport({
        url: rabbitUrl
    }));

    afterEach(() => transport.close());

    it('should consume queues', () => {
        transport.consumer({
            channelName: 'custom',
            exchangeName: 'task',
            queueName: 'command',
            consume() {}
        });

        let queueConsumed = false;
        const chan = transport.getChannel('custom');
        chan.consume = (...args) => {
            queueConsumed = true;
            return chan.get().consume(...args);
        };
        return transport.getReady()
            .then(() => {
                expect(queueConsumed).toBe(true);
            });
    });

    it('should maintain consumer tags', () => {
        const consumer = transport.consumer({
            channelName: 'custom',
            exchangeName: 'task',
            queueName: 'command',
            consumeOptions: {
                consumerTag: 'some-tag'
            },
            consume() {}
        });
        return transport.getReady()
            .then(() => {
                expect(consumer.consumerTag).toBe('some-tag');
            });
    });

});

