These tests require rabbitmq server running. Environment variable `RABBIT_URL` is used to connect to server.
Example of text command:

```
RABBIT_URL=amqp://192.168.99.100:5672 mocha
```

To run rabbitmq using docker use this command:

```
docker run -h rabbit \
  --name test-rabbit-server \
  -d \
  -e HOSTNAME=rabbit \
  -e RABBITMQ_NODENAME=rabbit \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  -p 5672:5672 \
  rabbitmq:3 
```

