'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('channel', () => {

    let transport = null;

    beforeEach(function() {
        transport = createTransport({
            url: rabbitUrl
        });
    });

    afterEach(function() {
        if (transport) {
            return transport.close()
                .then(() => transport = null);
        }
    });

    context('multiple channels consumption', function() {

        it('should be possible to consume multiple channels', function() {
            let alphaMsg = null;
            let bravoMsg = null;

            const client = transport.producer({
                exchangeName: 'channel.test'
            });

            startConsumer('alpha', msg => alphaMsg = msg);
            startConsumer('bravo', msg => bravoMsg = msg);

            function startConsumer(channelName, fn) {
                transport.consumer({
                    exchangeName: 'channel.test',
                    queueName: 'channel.test',
                    routes: ['default'],
                    consume(msg, job) {
                        fn(msg);
                        setTimeout(job.ack, 150);
                    },
                    channelName
                });
            }

            return transport.connect()
                .then(() => new Promise(resolve => {
                    client('hello alpha', 'default');
                    client('hello bravo', 'default');
                    setTimeout(resolve, 300);
                }))
                .then(() => {
                    expect(alphaMsg).toBe('hello alpha');
                    expect(bravoMsg).toBe('hello bravo');
                });
        });

    });

    context('prefetch', function() {

        it('should be configurable per-channel', () => {
            const alpha = transport.channel('alpha', {
                prefetchCount: 2,
                prefetchGlobal: true
            });
            expect(alpha.settings.prefetchCount).toBe(2);
            expect(alpha.settings.prefetchGlobal).toBe(true);
        });

        it('should default to count=1 global=false', () => {
            const bravo = transport.channel('bravo');
            expect(bravo.settings.prefetchCount).toBe(1);
            expect(bravo.settings.prefetchGlobal).toBe(false);
        });

    });

});

