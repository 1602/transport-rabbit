'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('producer', () => {

    let transport = null;

    beforeEach(() => {
        transport = createTransport({
            url: rabbitUrl
        });
    });

    afterEach(() => {
        transport.close();
    });

    it('should throw when called too early', () => {
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

