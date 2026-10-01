FROM node
ARG BUILD_DATE
ARG GIT_SHA=unknown
LABEL org.opencontainers.image.created=$BUILD_DATE
WORKDIR /app
COPY . .
RUN apt-get update
RUN apt-get install curl python3 \
    build-essential
RUN npm install
RUN npm run build
RUN rm -rf /var/lib/apt/lists/*
ADD https://example.com/tool.tar.gz /opt/tool.tar.gz
CMD ["node", "dist/server.js"]
