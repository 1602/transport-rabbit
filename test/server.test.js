'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('server', () => {

    let transport;
    let client;
    let result1;
    let result2;

    before(() => {
        transport = queueTransport({ url: rabbitUrl });

        client = transport.client({
            produce: {
                queue: {
                    exchange: 'task',
                    routes: [ 'command' ]
                }
            }
        });

        transport.server({
            consume: {
                queue: {
                    exchange: 'task',
                    routes: [ 'command' ],
                }
            },
            handler: {
                command: msg => {
                    if (msg) {
                        return 'hola';
                    }
                    throw new Error('Oops');
                }
            },
            produce: {
                queue: {
                    exchange: 'subtask',
                    routes: [ 'result', 'error' ]
                }
            }
        });

        transport.server({
            consume: {
                queue: {
                    exchange: 'subtask',
                    routes: [ 'result', 'error' ],
                }
            },
            handler: {
                result: res => result1 = res,
                error: err => result2 = err
            }
        });

        return transport.getReady();

    });

    after(() => transport.close());

    it('can produce results asyncronously', (done) => {
        client(1);
        setTimeout(() => {
            expect(result1).toEqual('hola');
            done();
        }, 300);
    });

    it('can produce results asyncronously', (done) => {
        client(0);
        setTimeout(() => {
            expect(result2.message).toEqual('Oops');
            done();
        }, 300);
    });

});

