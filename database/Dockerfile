FROM postgres:14.2-alpine

ENV POSTGRES_PASSWORD 1234

COPY db_dump_with_data.sql /docker-entrypoint-initdb.d/
