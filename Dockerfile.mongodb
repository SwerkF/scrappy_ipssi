FROM mongo:latest

COPY init-mongo.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init-mongo.sh

ENV MONGO_INITDB_ROOT_USERNAME=root
ENV MONGO_INITDB_ROOT_PASSWORD=password
ENV MONGO_INITDB_DATABASE=blogdumoderateur

EXPOSE 27017