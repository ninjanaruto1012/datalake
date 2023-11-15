FROM node:12-alpine
LABEL AUTHOR="PLuong"
WORKDIR /code
RUN npm install -g @angular/cli@12.2.18

COPY package*.json ./
RUN npm install