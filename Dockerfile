FROM node:slim

WORKDIR /usr/src/app
COPY ./package* .

RUN npm install

COPY . .

ENTRYPOINT ["node", "index.js"]
