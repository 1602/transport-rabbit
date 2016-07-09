'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('channel', () => {

    it('should send messages to queue');

    it('should publish messages to exchange');

    it('should allow to take care of stream throughput');

    describe('#prefetch', () => {

        context('global prefetch', () => {

            let transport, client;
            let alphaHello = '', bravoHello = '';

            before(() => {
                transport = queueTransport({
                    url: rabbitUrl,
                    prefetch: 1,
                    channelConfig: {
                        alpha: {
                            prefetch: {
                                count: 1,
                                global: true
                            }
                        },
                        bravo: {
                            prefetch: {
                                count: 1,
                                global: true
                            }
                        }
                    }
                });

                client = transport.createCommandSender('task');

                startServer('alpha', msg => alphaHello = msg);
                startServer('bravo', msg => bravoHello = msg);

                function startServer(channel, fn) {
                    transport.createCommandServer('task', (msg, job) => {
                        fn(msg);
                        setTimeout(job.accept, 150);
                    }, {
                        channel,
                        produceResults: false
                    });
                }

                transport.createCommandServer('task', (msg, job) => {
                    bravoHello = msg;
                    setTimeout(job.accept, 150);
                }, {
                    channel: 'bravo',
                    produceResults: false
                });

                return transport.getReady();
            });

            after(() => transport.close());

            it.only('should be possible to consume multiple channels', () => {
                client('hello alpha');
                client('hello bravo');
                return new Promise(resolve => setTimeout(resolve, 100))
                    .then(() => {
                        expect(alphaHello).toBe('hello alpha');
                        expect(bravoHello).toBe('hello bravo');
                        return new Promise(resolve => setTimeout(resolve, 300));
                    })
                    .then(() => transport.close());

            });
        });
    });

});

