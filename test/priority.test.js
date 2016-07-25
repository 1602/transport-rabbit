'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('priority queues', function() {

    let transport = null;

    beforeEach(function() {
        transport = createTransport({
            url: rabbitUrl
        });
        const channel = transport.channel('custom');
        channel.init(() => channel.assertExchange('priority.test'));
        channel.init(() => channel.assertQueue('priority.test', {
            maxPriority: 10
        }));
        channel.init(() =>
            channel.bindQueue('priority.test', 'priority.test', 'default'));
    });

    afterEach(function() {
        const channel = transport.channel('custom');
        return channel.purgeQueue('priority.test');
    });

    afterEach(function() {
        return transport.close();
    });

    it('prefer higher priority messages after nack', function() {

        const messages = [];

        let nackedOnce = false;

        const produce = transport.producer({
            exchangeName: 'priority.test'
        });

        transport.consumer({
            channelName: 'custom',
            queueName: 'priority.test',
            consume(msg, job) {
                if (!nackedOnce) {
                    nackedOnce = true;
                    return job.nack();
                }
                messages.push(msg);
                job.ack();
            }
        });

        return transport.connect()
            .then(() => produce('irrelevant', 'default', { priority: 1 }))
            .then(() => produce('important', 'default', { priority: 10 }))
            .then(() => new Promise(resolve => setTimeout(resolve, 300)))
            .then(() => {
                expect(messages[0]).toBe('important');
                expect(messages[1]).toBe('irrelevant');
            });
    });

});

