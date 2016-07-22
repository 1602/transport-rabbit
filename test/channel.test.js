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
        return transport.close();
    });

    it('should allocate amqlib channel on connect', function() {
        const channel = transport.channel('default');
        expect(channel.getWrappedChannel).toThrow();
        return transport.connect()
            .then(() => expect(channel.getWrappedChannel).toNotThrow());
    });

    it('should close amqplib channel on close', function() {
        const channel = transport.channel('default');
        return transport.connect()
            .then(() => expect(transport.channels.default).toExist())
            .then(() => channel.close())
            .then(() => expect(transport.channels.default).toNotExist());
    });

    context('multiple channels consumption', function() {

        it('should be possible to consume multiple channels', function() {
            let alphaMsg = null;
            let bravoMsg = null;

            const channel = transport.channel('default');

            const client = transport.producer({
                exchangeName: 'channel.test',
                channelName: 'default'
            });

            transport.addInit(() =>
                channel.assertExchange('channel.test', 'direct'));

            transport.addInit(() =>
                channel.assertQueue('channel.test'));

            transport.addInit(() =>
                channel.bindQueue('channel.test', 'channel.test', 'default'));

            startConsumer('alpha', msg => alphaMsg = msg);
            startConsumer('bravo', msg => bravoMsg = msg);

            function startConsumer(channelName, fn) {
                transport.consumer({
                    queueName: 'channel.test',
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

        it('should be configurable per-channel', function() {
            const alpha = transport.channel('alpha', {
                prefetchCount: 2,
                prefetchGlobal: true
            });
            expect(alpha.settings.prefetchCount).toBe(2);
            expect(alpha.settings.prefetchGlobal).toBe(true);
        });

        it('should default to count=1 global=false', function() {
            const bravo = transport.channel('bravo');
            expect(bravo.settings.prefetchCount).toBe(1);
            expect(bravo.settings.prefetchGlobal).toBe(false);
        });

    });

});

