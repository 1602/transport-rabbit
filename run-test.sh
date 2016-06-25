#!/bin/sh

echo "node 4"

docker run \
  --rm \
  -v $(pwd):/src/ \
  -e TERM=xterm \
  -e RABBIT_URL=amqp://rabbit:5672/ \
  --link test-rabbit-server \
  -w="/src" mhart/alpine-node:4 /src/node_modules/.bin/mocha -c -R landing

echo "node 5"

docker run \
  --rm \
  -v $(pwd):/src/ \
  -e TERM=xterm \
  -e RABBIT_URL=amqp://rabbit:5672/ \
  --link test-rabbit-server \
  -w="/src" mhart/alpine-node:5 /src/node_modules/.bin/mocha -c -R nyan

echo "node 6"

docker run \
  --rm \
  -v $(pwd):/src/ \
  -e TERM=xterm \
  -e RABBIT_URL=amqp://rabbit:5672/ \
  --link test-rabbit-server \
  -w="/src" mhart/alpine-node:6 /src/node_modules/.bin/mocha -c -R list

