[![Coverage Status](https://coveralls.io/repos/github/1602/transport-rabbit/badge.svg?branch=master)](https://coveralls.io/github/1602/transport-rabbit?branch=master)

# Usage

This is an example of low-level API implemented by this library

```
const transport = queueTransport({ url: 'amqp://localhost:5672/' });

// on server
transport.server({
    consume: {
        queue: {
            exchange: 'log',
            routes: [ 'warn', 'info', 'error' ],
            options: {
                durable: true
            }
        }
    },
    handler: {
        warn: msg => console.warn(msg),
        info: msg => console.info(msg),
        error: msg => console.error(msg)
    }
});

// on client
const send = transport.client({
    produce: {
        queue: {
            exchange: 'log',
            routes: [ 'warn', 'info', 'error' ],
            options: {
                durable: true
            }
        }
    }
});

transport.getReady()
    .then(() => {
        send('Hello World', 'warn');
    });
```

But low level API only good for understanding how things actually work under
the hood. It is recommended to use higher level API which predefine some
messaging patterns such as pubsub, RPC or basic producer-consumer queues.

An example of higher-level API

```
const transport = queueTransport({ url: 'amqp://localhost:5672' });

// A: producer
const send = transport.createCommandSender('multiply-by-2');
transport.getReady().then(() => send(10));

// B: consumer
transport.createCommandServer('multiply-by-2', param => {
    return param * 2;
});

// C: consumer of results produced by consumer (B)
transport.createCommandResultRecipient('task', {
    result: num => console.log('got result %d', num),
    error: err => console.log('got error', err)
});
```

Higher level API [implements](./domains/command.js) most common scenarios of
messaging between nodes and use lower level API internally.

# Why not just use amqplib?

What problem this library is trying to solve? API provided by amqplib is
awesome, but it requires some boilerplate code to be written every time we want
to connect to the queue server. Usual workflow looks like that:

1. open connection
2. create channel
3. set prefetch for channel (if we're going to consume some queues)
4. assert exchanges
5. assert queues
6. bind exchanges to queues
7. start consuming / producing messages from asserted queues / exchanges

In applications we usually interested only in messaging and not in all
accidental complexity coming along with amqp protocol. Even if this is
necessary to be able to use multiple channels, configure prefetch and handle
logic of reconnecting in case of disconnects this should not be concern of our
code, because if should be written every time in every node we want to connect
to the queue.

# Tests

```
npm run docker-test
```

check [./test/README.md](./test/README.md) for more details on testing

