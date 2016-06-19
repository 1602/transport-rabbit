FROM 1602/alpine-node:4

COPY package.json /src/package.json

COPY . /src

WORKDIR /src
VOLUME /work

EXPOSE  3000
USER root

CMD ["node", "index.js"]

