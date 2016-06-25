'use strict';

const DEFAULT_RECONNECT_TIMEOUT = 2000;

const amqplib = require('amqplib');
const EventEmitter = require('events');

const debug = require('debug')('rabbit:connection');

module.exports = init;

function init(settings) {

    const events = new EventEmitter();

    let reconnect = settings.reconnect;
    let state = 'disconnected';
    let latestConnection = null;

    setupConnection(connect(settings));

    return {
        events,
        isConnected,
        isDisconnected,
        createChannel,

        close() {
            reconnect = false;
            if (!isDisconnected()) {
                return closeConnection();
            }
        },

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

    function closeConnection() {
        state = 'disconnected';
        return latestConnection && latestConnection.close();
    }

    function connect(settings) {
        return amqplib.connect(settings.url)
            .catch(err => {
                if (reconnect) {
                    debug('Error while connecting, will try to reconnect', err);

                    return new Promise(r => setTimeout(() => r(connect(settings)), settings.reconnectTimeout || DEFAULT_RECONNECT_TIMEOUT));
                }

                throw err;
            });
    }

    function setupConnection(connection) {
        return Promise.resolve(connection)
            .then(connection => {
                connection.on('error', err => {
                    console.error('Connection error', err);
                });
                connection.on('close', () => {
                    state = 'disconnected';
                    if (reconnect) {
                        debug('Connection closed. Will reconnect in a moment');
                        setTimeout(() => {
                            setupConnection(connect(settings));
                        }, settings.reconnectTimeout || DEFAULT_RECONNECT_TIMEOUT);
                    } else {
                        debug('Connection closed. Will not reconnect');
                    }
                });
                latestConnection = connection;
                state = 'connected';
                events.emit('connected');
                return connection;
            });
    }

}

