'use strict';

const assert = require('assert');
const amqplib = require('amqplib');
const debug = require('debug')('rabbit:transport');
const EventEmitter = require('events');

const createFactories = require('./factories');

/**
 * @param settings {Object}
 * @param settings.url {String} rabbitmq url
 * @param settings.quitGracefullyOnTerm {Boolean} (false) attach SIGTERM/SIGINT handlers
 * @param settings.reconnect {Boolean} (false) reconnect on connection failures
 * @param settings.reconnectInterval {Number} (2000) reconnect interval
 */
module.exports = function createTransport(settings) {
    assert(settings, 'settings not specified');

    const {
        url,
        reconnectInterval = 2000,
        quitGracefullyOnTerm = false
    } = settings;

    let {
        reconnect = false
    } = settings;

    if (quitGracefullyOnTerm) {
        process.once('SIGTERM', close);
        process.once('SIGINT', close);
    }

    const events = new EventEmitter();
    const channels = {};
    const initializers = [];

    let connection = null;
    let initialized = false;
    let currentInitializer = Promise.resolve(); // for sequential run
    
    const transport = {
        events,
        channels,
        settings,
        connect,
        getReady: connect,      // TODO there can be only one †
        close,
        getConnection,
        isConnected,
        addInit
    };
    
    Object.assign(transport, createFactories(transport));
    
    return transport;

    function connect() {
        if (isConnected()) {
            return Promise.resolve();
        }
        return Promise.resolve()
            .then(() => _connect())
            .then(conn => {
                debug('connected');
                events.emit('connected');
                connection = conn;
                connection.on('error', err => {
                    console.error('Connection error', err);
                    connection.close();
                });
                connection.on('close', () => {
                    connection = null;
                    if (reconnect) {
                        debug('connection closed, will reconnect');
                        setTimeout(() => connect(settings), reconnectInterval);
                    } else {
                        debug('connection closed, will NOT reconnect');
                    }
                });
                return init();
            });
    }

    function _connect() {
        debug('connecting to %s', url);
        return amqplib.connect(url)
            .catch(err => {
                if (reconnect) {
                    debug('error while connecting, will try to reconnect', err);
                    return new Promise(resolve =>
                        setTimeout(() => resolve(_connect()), reconnectInterval));
                }
                debug('connection error', err);
                connection = null;
                throw err;
            });
    }
    
    function getConnection() {
        assert(connection, 'RabbitMQ not connected');
        return connection;
    }

    function isConnected() {
        return !!connection;
    }

    function close() {
        if (!isConnected()) {
            return Promise.resolve();
        }
        reconnect = false;
        return Promise.resolve()
            .then(() => connection.close())
            .then(() => {
                debug('connection closed manually');
                connection = null;
                events.emit('close');
            });
    }

    function addInit(fn) {
        if (initialized) {
            execInit(fn)
                .catch(err => onInitError(err));
        }
        initializers.push(fn);
        return function removeInit() {
            const i = initializers.indexOf(fn);
            if (i > -1) {
                initializers.splice(i, 1);
            }
        };
    }
    
    function init() {
        initialized = false;
        // imperial loops! b/c we can!
        initializers.forEach(fn => execInit(fn));
        return currentInitializer
            .then(() => {
                initialized = true;
            }, err => onInitError(err));
    }
    
    function execInit(fn) {
        currentInitializer = currentInitializer.then(() => fn());
        return currentInitializer;
    }

    function onInitError(err) {
        console.error('Error during initialization', err);
        return connection.close(err);
    }

};
