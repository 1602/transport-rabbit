'use strict';

const DEFAULT_RECONNECT_TIMEOUT = 2000;

const amqplib = require('amqplib');
const EventEmitter = require('events');

const debug = require('debug')('rabbit:connection');

module.exports = init;

function init(settings) {

    const events = new EventEmitter();

    let reconnect = settings.reconnect;
    let reconnectTimeout;
    let state = 'disconnected';
    let latestConnection = null;

    if (settings.quitGracefullyOnTerm) {
        process.once('SIGTERM', close);
        process.once('SIGINT', close);
    }

    setupConnection(connect(settings));

    return {
        events,
        isConnected,
        isDisconnected,
        createChannel,
        close,
        forceClose: () => closeConnection()
    };

    function isDisconnected() {
        return state === 'disconnected';
    }

    function isConnected() {
        return state === 'connected';
    }

    function createChannel() {
        return latestConnection && latestConnection.createChannel();
    }

    function close() {
        reconnect = false;
        if (isDisconnected()) {
            events.emit('close');
        } else {
            return closeConnection();
        }
    }

    function closeConnection() {
        state = 'disconnected';
        return latestConnection && latestConnection.close();
    }

    function connect(settings) {
        reconnectTimeout = settings.reconnectTimeout || DEFAULT_RECONNECT_TIMEOUT;
        return amqplib.connect(settings.url)
            .catch(err => {
                if (reconnect) {
                    debug('Error while connecting, will try to reconnect', err);

                    return new Promise(r => setTimeout(() => r(connect(settings)), reconnectTimeout));
                }

                throw err;
            });
    }

    function setupConnection(connection) {
        return Promise.resolve(connection)
            .then(connection => {
                connection.on('error', err => {
                    debug('Connection error', err);
                });
                connection.on('close', () => {
                    state = 'disconnected';
                    if (reconnect) {
                        debug('Connection closed. Will reconnect in a moment');
                        setTimeout(() => setupConnection(connect(settings)), reconnectTimeout);
                    } else {
                        debug('Connection closed. Will not reconnect');
                        events.emit('close');
                    }
                });
                latestConnection = connection;
                state = 'connected';
                events.emit('connected');
                return connection;
            });
    }

}

