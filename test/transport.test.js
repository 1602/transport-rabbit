'use strict';

const expect = require('expect');
const createTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.100:5672';

/* eslint max-nested-callbacks: [2, 6] */

describe('transport', function() {

    let transport;

    afterEach(function() {
        if (transport) {
            return transport.close()
        }
    });

    it('should connect', function() {
        transport = createTransport({ url: rabbitUrl });

        expect(transport.isConnected()).toBe(false);
        return transport.connect()
            .then(() => {
                expect(transport.isConnected()).toBe(true);
            });
    });

    it.only('should emit "connected"', function(done) {
        transport = createTransport({ url: rabbitUrl });
        transport.connect();
        transport.events.once('connected', done);
    });

    it('should reconnect in case of disconnect', function(done) {
        transport = createTransport({
            url: rabbitUrl,
            reconnect: true,
            reconnectInterval: 100
        });
        return transport.connect()
            .then(() => {
                transport.getConnection().close();
                transport.events.once('connected', () => done());
            });
    });

    it('should try to reconnect while server is not available', done => {
        const settings = {
            url: 'amqp://localhost:9999/',
            reconnect: true,
            reconnectTimeout: 100
        };
        transport = createTransport(settings);
        Promise.resolve().then(() => settings.url = rabbitUrl);
        transport.events.once('ready', done);
    });

    it('should not try to reconnect if not configured', done => {
        const settings = {
            url: 'amqp://localhost:9999/',
            /* reconnect: false, -- default */
            reconnectTimeout: 10
        };
        transport = createTransport(settings);
        Promise.resolve().then(() => settings.url = rabbitUrl);
        transport.events.once('ready', () => {
            done(new Error('Unexpected connect'));
        });
        setTimeout(done, 100);
    });

    it('should quit gracefully on SIGINT and SIGTERM when configured', () => {
        transport = createTransport({
            url: rabbitUrl,
            quitGracefullyOnTerm: true
        });

        return transport.getReady()
            .then(() => process.emit('SIGTERM'))
            .then(() => getClose());

        function getClose() {
            return new Promise(resolve => transport.events.on('close', resolve));
        }

    });

    it('should close connection when channel can not be opened', done => {
        transport = createTransport({ url: rabbitUrl });
        const close = transport.connection.close;
        const createChannel = transport.connection.createChannel;

        transport.connection.createChannel = () =>
            Promise.reject(new Error('Too many channels opened'));

        transport.connection.close = () => {
            transport.connection.createChannel = createChannel;
            transport.connection.close = close;
            transport.connection.close();
            transport = null;
            done();
        };
    });

    it('should expose errors thrown during init', done => {
        transport = createTransport({ url: rabbitUrl });
        transport.connection.createChannel = () =>
            Promise.reject(new Error('Too many channels opened'));
        transport.events.once('error', err => {
            expect(err.message).toBe('Too many channels opened');
            done();
        });
    });

    it('getReady should be idempotent', () => {
        transport = createTransport({ url: rabbitUrl });
        return Promise.resolve()
            .then(() => transport.getReady())
            .then(() => transport.getReady());
    });

    it('close should be idempotent', () => {
        return Promise.resolve()
            .then(() => transport.close())
            .then(() => transport.close());
    });

});

