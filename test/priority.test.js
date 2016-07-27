'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('priority queues', () => {

    let transport = null;

    beforeEach(() => {
        transport = createTransport({
            url: rabbitUrl
        });
        const channel = transport.channel('custom');
        channel.addInit(() => channel.assertExchange('priority.test'));
        channel.addInit(() => channel.assertQueue('priority.test', {
            maxPriority: 10
        }));
        channel.addInit(() =>
            channel.bindQueue('priority.test', 'priority.test', 'default'));
    });

    afterEach(() => {
        const channel = transport.channel('custom');
        return channel.purgeQueue('priority.test');
    });

    afterEach(() => {
        return transport.close();
    });

    it('prefer higher priority messages after nack', () => {

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

        return transport.getReady()
            .then(() => produce('irrelevant', 'default', { priority: 1 }))
            .then(() => produce('important', 'default', { priority: 10 }))
            .then(() => new Promise(resolve => setTimeout(resolve, 300)))
            .then(() => {
                expect(messages[0]).toBe('important');
                expect(messages[1]).toBe('irrelevant');
            });
    });

});

