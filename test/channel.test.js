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
                        default: {
                            prefetch: { count: 2 }
                        },
                        alpha: {
                            prefetch: { global: true }
                        },
                        bravo: {
                            prefetch: { count: 1, global: true }
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

                return transport.getReady()
                    .then(() => {
                        client('hello alpha');
                        client('hello bravo');
                        return new Promise(resolve => setTimeout(resolve, 100));
                    });
            });

            it('should be possible to consume multiple channels', done => {
                expect(alphaHello).toBe('hello alpha');
                expect(bravoHello).toBe('hello bravo');
                setTimeout(done, 300);
            });

            it('defaults global flag to false', () => {
                const settings = transport.getChannel('default').getSettings();
                expect(settings.prefetchIsGlobal).toBe(false);
            });

            it('defaults prefetch count to 1', () => {
                const settings = transport.getChannel('alpha').getSettings();
                expect(settings.prefetchCount).toBe(1);
            });

            it('allows to configure global prefetch', () => {
                const settings = transport.getChannel('alpha').getSettings();
                expect(settings.prefetchIsGlobal).toBe(true);
            });

            after(() => transport.close());

        });
    });

});

