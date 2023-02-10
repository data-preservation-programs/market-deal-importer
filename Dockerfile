FROM public.ecr.aws/docker/library/node:16
RUN apt-get update && apt-get install -y aria2 zstd
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY ./dist .
CMD aria2c -x 10 -s 10 https://marketdeals.s3.amazonaws.com/StateMarketDeals.json.zst && unzstd StateMarketDeals.json.zst && node index.js
