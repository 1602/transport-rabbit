'use strict';

const expect = require('expect');
const createTransport = require('../');
const sinon = require('sinon');
const amqplib = require('amqplib');

const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.100:5672';

describe('transport', () => {

    let transport;

    afterEach(() => {
        if (transport) {
            return transport.close()
                .then(() => transport = null);
        }
    });

    it('should connect', () => {
        transport = createTransport({ url: rabbitUrl });

        expect(transport.isConnected()).toBe(false);
        return transport.connect()
            .then(() => {
                expect(transport.isConnected()).toBe(true);
            });
    });

    it('should emit "connected"', done => {
        transport = createTransport({ url: rabbitUrl });
        transport.connect();
        transport.events.once('connected', done);
    });

    it('should reconnect in case of disconnect', done => {
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
        transport = createTransport({
            url: rabbitUrl,
            reconnect: true,
            reconnectInterval: 100
        });
        sinon.stub(amqplib, 'connect', () =>
            Promise.reject(new Error('connection failed')));
        transport.connect();
        setTimeout(() => amqplib.connect.restore(), 100);
        transport.events.once('connected', done);
    });

    it('should not try to reconnect if not configured', done => {
        transport = createTransport({
            url: rabbitUrl,
            /* reconnect: false, default */
            reconnectInterval: 10
        });
        transport.connect()
            .then(() => {
                transport.getConnection().close();
                transport.events.once('connected', () =>
                    done(new Error('Unexpected connect')));
            });
        setTimeout(done, 200);
    });

    it('should quit gracefully on SIGINT and SIGTERM when configured', () => {
        transport = createTransport({
            url: rabbitUrl,
            quitGracefullyOnTerm: true
        });
        return transport.connect()
            .then(() => new Promise(resolve => {
                transport.events.once('close', resolve);
                process.emit('SIGTERM');
            }));
    });

    it('connect should be idempotent', () => {
        transport = createTransport({ url: rabbitUrl });
        return Promise.resolve()
            .then(() => transport.connect())
            .then(() => transport.connect());
    });

    it('close should be idempotent', () => {
        transport = createTransport({ url: rabbitUrl });
        return Promise.resolve()
            .then(() => transport.connect())
            .then(() => transport.close())
            .then(() => transport.close());
    });

});

