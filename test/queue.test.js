'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('queue', () => {

    let transport;

    beforeEach(done => {
        transport = queueTransport({
            url: rabbitUrl,
            reconnect: false
        });

        transport.events.once('ready', () => {
            done();
        });

    });

    afterEach(() => {
        return transport.close();
    });

    describe('#messageCount', () => {

        context('existing queue', () => {

            beforeEach(() => {
                return transport.queue.assert('q');
            });

            afterEach(() => {
                return transport.queue.delete('q');
            });

            it('should check length of queue', () => transport.queue.messageCount('q')
               .then(messagesCount => expect(messagesCount).toEqual(0))
            );

        });

        context('not existing queue', () => {

            beforeEach(() => {
                return transport.queue.delete('q');
            });

            it('should result in error', () => transport.queue.messageCount('q')
                .then(() => {
                    throw new Error('Unexpected normal continuation');
                })
                .catch(err => expect(err.message).toContain('NOT-FOUND'))
            );

        });

    });

    describe('#consumerCount', () => {

        it('should check consumers count for the queue', () => {
            let consumerTag = '';
            return transport.queue.assert('q')
                .then(() => transport.queue.consumerCount('q'))
                .then(consumerCount => expect(consumerCount).toBe(0))
                .then(() => transport.queue.consume('q', () => {}))
                .then((c) => consumerTag = c.consumerTag)
                .then(() => transport.queue.consumerCount('q'))
                .then(consumerCount => expect(consumerCount).toBe(1))
                .then(() => transport.channel.cancel(consumerTag))
                .then(() => transport.queue.consumerCount('q'))
                .then(consumerCount => expect(consumerCount).toBe(0));
        });

    });

});

