'use strict';

const EventEmitter = require('events');
const amqplib = require('amqplib');
const debug = require('debug')('rabbit:transport');

const createChannel = require('./channel');
const createFactories = require('./factories');

/**
 * @param settings {Object}
 * @param settings.url {String} rabbitmq url
 * @param settings.quitGracefullyOnTerm {Boolean} (true) attach SIGTERM/SIGINT handlers
 * @param settings.reconnect {Boolean} (true) reconnect on connection failures
 * @param settings.reconnectInterval {Number} (2000) reconnect interval
 * @param settings.channelSettings — channel settings (global and per-channelName)
 * @param settings.channelSettings.prefetchCount {Number} (1)
 * @param settings.channelSettings.prefetchGlobal {Boolean} (false)
 * @param settings.channelSettings.<chanName>.prefetchCount
 * @param settings.channelSettings.<chanName>.prefetchGlobal
 */
module.exports = function createTransport(settings) {
    assert(settings, 'settings not specified');

    const {
        url,
        reconnectInterval = 2000,
        quitGracefullyOnTerm = true
    } = settings;

    let {
        reconnect = true
    } = settings;

    if (quitGracefullyOnTerm) {
        process.once('SIGTERM', close);
        process.once('SIGINT', close);
    }

    const channels = {};
    const events = new EventEmitter();

    let connection = null;
    
    const transport = {
        events,
        settings,
        connect,
        close,
        get connection() {
            return connection;
        },
        isConnected,
        assertChannel
    };
    
    Object.assign(transport, createFactories(transport));
    
    return transport;

    function connect() {
        if (connection) {
            return Promise.resolve();
        }
        debug('connecting to %s', url);
        amqplib.connect(url)
            .catch(err => {
                if (reconnect) {
                    debug('error while connecting, will try to reconnect', err);
                    return new Promise(resolve =>
                        setTimeout(() => resolve(connect()), reconnectInterval));
                }
                debug('connection error', err);
                connection = null;
                throw err;
            })
            .then(conn => _setupConnection(conn));
    }

    function isConnected() {
        return !!connection;
    }

    function close() {
        if (!isConnected()) {
            events.emit('close');
            return Promise.resolve();
        }
        reconnect = false;
        return Promise.resolve()
            .then(() => connection.close())
            .then(() => {
                debug('connection closed manually');
                connection = null;
            });
    }

    function _setupConnection(conn) {
        debug('connected');
        events.emit('connected');
        connection = conn;
        connection.on('error', err => {
            console.error(err);
            connection.close();
        });
        connection.on('close', () => {
            if (reconnect) {
                debug('connection closed, will reconnect');
                setTimeout(() => connect(settings), reconnectInterval);
            } else {
                debug('connection closed, will NOT reconnect');
                events.emit('close');
            }
        });
    }

    function assertChannel(channelName) {
        assert(channelName, 'channelName not specified');
        if (!(channelName in channels)) {
            channels[channelName] = createChannel(transport, channelName);
        }
        return channels[channelName];
    }
    
};
