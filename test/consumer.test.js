'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('consumer', function() {

    let transport = null;

    beforeEach(function() {
        transport = createTransport({
            url: rabbitUrl
        });
    });

    afterEach(function() {
        return transport.close();
    });

    it('should consume queues', function() {
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

    it('should maintain consumer tags', function() {
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

