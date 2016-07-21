'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

describe('command', () => {

    context('normal flow', () => {

        let transport;
        let client;
        let result1;
        let context1;
        let result2;
        let context2;

        before(() => {
            transport = queueTransport({ url: rabbitUrl });

            client = transport.commandSender('task');

            transport.commandResultRecipient('task', {

                error: (err, job) => {
                    result2 = err;
                    context2 = job.context;
                },

                result: (res, job) => {
                    result1 = res;
                    context1 = job.context;
                }

            });

            transport.commandServer('task', {
                handler(msg, job) {
                    job.ack();

                    if (msg) {
                        return 'hola';
                    }

                    throw new Error('Oops');
                }
            });

            return transport.getReady();

        });

        after(() => transport.close());

        it('can produce results asyncronously', (done) => {
            client(1, { context: { say: 'hello' } });
            setTimeout(() => {
                expect(result1).toEqual('hola');
                expect(context1).toEqual({ say: 'hello' });
                done();
            }, 300);
        });

        it('can produce errors asyncronously', (done) => {
            client(0);
            setTimeout(() => {
                expect(result2.message).toEqual('Oops');
                expect(context2).toEqual(null);
                done();
            }, 300);
        });

    });

    describe('ack', () => {

        let transport = null;
        let send;
        let handler;

        beforeEach(() => {
            transport = queueTransport({ url: rabbitUrl });
            send = transport.commandSender('task-donotcare');

            transport.commandServer('task-donotcare', {
                handler: function() {
                    return handler.apply(null, [].slice.call(arguments));
                }
            });

            return transport.getReady();
        });

        afterEach(() => {
            return transport.close();
        });

        it('should allow to nack', done => {
            let rejectedOnce = false;
            handler = (param, job) => {
                expect(param).toBe('hello');
                expect(typeof job.ack).toBe('function');
                expect(typeof job.nack).toBe('function');
                if (rejectedOnce) {
                    job.ack();
                    expect(job.ack).toNotThrow();
                    expect(job.nack).toNotThrow();
                    done();
                    return;
                }
                rejectedOnce = true;
                job.nack();
            };
            send('hello');
        });
    });

});

