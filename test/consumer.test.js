'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('consumer', () => {

    let transport = null;

    beforeEach(() => transport = createTransport({
        url: rabbitUrl
    }));

    afterEach(() => transport.close());

    it('should consume queues', () => {
        transport.consumer({
            channelName: 'custom',
            exchangeName: 'consumer.test',
            queueName: 'consumer.test',
            consume() {}
        });

        let queueConsumed = false;
        const chan = transport.channel('custom');
        chan.consume = (...args) => {
            queueConsumed = true;
            return chan.getWrappedChannel().consume(...args);
        };
        return transport.connect()
            .then(() => {
                expect(queueConsumed).toBe(true);
            });
    });

    it('should maintain consumer tags', () => {
        const consumer = transport.consumer({
            channelName: 'custom',
            exchangeName: 'consumer.test',
            queueName: 'consumer.test',
            consumeOptions: {
                consumerTag: 'some-tag'
            },
            consume() {}
        });
        return transport.connect()
            .then(() => {
                expect(consumer.consumerTag).toBe('some-tag');
            });
    });

});

