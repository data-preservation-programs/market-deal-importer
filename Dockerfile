FROM public.ecr.aws/docker/library/node:16
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY ./dist .
CMD ["node", "index.js"]
