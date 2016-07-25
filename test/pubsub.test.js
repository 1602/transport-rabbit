'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */
describe('pubsub', function() {

    let transport;

    beforeEach(function() {
        transport = queueTransport({ url: rabbitUrl });
    });

    afterEach(function() {
        return transport.close();
    });

    it('should receive message to both queues', function() {
        const publish = transport.publisher('pubsub.test');

        const results1 = [];
        const results2 = [];

        transport.subscriber('pubsub.test', {
            consume: res => results1.push(res)
        });

        transport.subscriber('pubsub.test', {
            consume: res => results2.push(res)
        });

        return transport.getReady()
            .then(() => publish('message'))
            .then(() => new Promise(resolve => setTimeout(resolve, 300)))
            .then(() => {
                expect(results1[0]).toBe('message');
                expect(results2[0]).toBe('message');
            });
    });

    it('should work like router', function() {
        const publish = transport.publisher('pubsub.test');

        const infos = [];
        const errors = [];
        const all = [];

        transport.subscriber('pubsub.test', {
            topic: 'info',
            consume: res => infos.push(res)
        });

        transport.subscriber('pubsub.test', {
            topic: 'error',
            consume: res => errors.push(res)
        });

        transport.subscriber('pubsub.test', {
            topic: '#',
            consume: res => all.push(res)
        });

        return transport.getReady()
            .then(() => publish('hey', 'info'))
            .then(() => publish('hola', 'info'))
            .then(() => publish('oops', 'error'))
            .then(() => new Promise(resolve => setTimeout(resolve, 300)))
            .then(() => {
                expect(infos.length).toBe(2);
                expect(errors.length).toBe(1);
                expect(all.length).toBe(3);
            });
    });

});

