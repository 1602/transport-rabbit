'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('producer', () => {

    let transport = null;

    beforeEach(() => transport = queueTransport({
        url: rabbitUrl
    }));

    afterEach(() => transport.close());

    it('should assert exchanges', () => {
        transport.producer({
            channelName: 'custom',
            exchangeName: 'task',
            exchangeType: 'direct',
        });

        let exchangeAsserted = false;
        const chan = transport.getChannel('custom');
        chan.assertExchange = () => {
            exchangeAsserted = true;
            return Promise.resolve();
        };

        return transport.getReady()
            .then(() => {
                expect(exchangeAsserted).toBe(true);
            });
    });

});

