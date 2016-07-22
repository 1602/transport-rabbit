'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('producer', function() {

    let transport = null;

    beforeEach(function() {
        transport = createTransport({
            url: rabbitUrl
        });
    });

    afterEach(function() {
        transport.close();
    });

    it('should assert exchanges', function() {
        transport.producer({
            channelName: 'custom',
            exchangeName: 'producer.test',
            exchangeType: 'direct'
        });

        let exchangeAsserted = false;
        const chan = transport.channel('custom');

        chan.assertExchange = () => {
            exchangeAsserted = true;
            return Promise.resolve();
        };

        return transport.connect()
            .then(() => {
                expect(exchangeAsserted).toBe(true);
            });
    });

    it('should throw when called too early', function() {
        const produce = transport.producer({
            exchangeName: 'log'
        });
        return Promise.resolve()
            .then(() => produce('hello'))
            .then(() => {
                throw new Error('Unexpected success');
            }, err => expect(err.message).toBe('Transport not connected'));
    });

});

