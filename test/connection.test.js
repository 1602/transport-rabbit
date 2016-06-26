'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('connection', () => {

    let transport;

    afterEach((done) => {
        if (transport) {
            Promise.resolve(transport.close()).then(done, done);
        } else {
            done();
        }
    });

    it('should trigger event.ready when connected and setup', function(done) {
        transport = queueTransport({ url: rabbitUrl });

        transport.events.once('ready', function() {
            try {
                expect(arguments.length).toEqual(0);
                expect(transport.isConnected()).toBe(true);
                done();
            } catch(e) {
                done(e);
            }
        });
    });

    it('should reconnect in case of disconnect', done => {
        transport = queueTransport({
            url: rabbitUrl,
            reconnect: true,
            reconnectTimeout: 100
        });

        transport.events.once('ready', () => {
            transport.queue.check('not-existing-queue')
                .catch(() => {});
            transport.events.once('ready', () => done());
        });
    });

    it('should try to reconnect while server is not available', done => {
        const settings = {
            url: 'amqp://localhost:9999/',
            reconnect: true,
            reconnectTimeout: 100
        };
        transport = queueTransport(settings);
        Promise.resolve().then(() => settings.url = rabbitUrl);
        transport.events.once('ready', done);
    });

    it('should not try to reconnect if not configured', done => {
        const settings = {
            url: 'amqp://localhost:9999/',
            /* reconnect: false, -- default */
            reconnectTimeout: 10
        };
        transport = queueTransport(settings);
        Promise.resolve().then(() => settings.url = rabbitUrl);
        transport.events.once('ready', () => {
            done(new Error('Unexpected connect'));
        });
        setTimeout(done, 500);
    });

});
