'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('channel', () => {

    it('should send messages to queue');

    it('should publish messages to exchange');

    it('should allow to take care of stream throughput');

    it('should be possible to consume multiple channels', () => {
        const transport = queueTransport({ url: rabbitUrl, prefetch: 1 });

        const client = transport.createCommandSender('task');

        let alphaHello = '', bravoHello = '';

        transport.createCommandServer('task', function(msg, job) {
            alphaHello = msg;
            setTimeout(job.accept, 150);
        }, {
            channel: 'alpha',
            produceResults: false
        });

        transport.createCommandServer('task', function(msg, job) {
            bravoHello = msg;
            setTimeout(job.accept, 150);
        }, {
            channel: 'bravo',
            produceResults: false
        });

        return transport.getReady()
            .then(() => {
                client('hello alpha');
                client('hello bravo');
                return new Promise(resolve => setTimeout(resolve, 100));
            })
            .then(() => {
                expect(alphaHello).toBe('hello alpha');
                expect(bravoHello).toBe('hello bravo');
                return new Promise(resolve => setTimeout(resolve, 300));
            })
            .then(() => transport.close());

    });

});

