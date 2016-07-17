These tests require rabbitmq server running.
Environment variable `RABBIT_URL` is used to connect to server.

Example of test command:

```
RABBIT_URL=amqp://192.168.99.100:5672 npm test
```

To run rabbitmq using docker just use `npm run start-test-rabbit`.


