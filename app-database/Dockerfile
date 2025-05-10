FROM postgres:15

# Copy initialization scripts
COPY init/ /docker-entrypoint-initdb.d/

# Environment variables
ENV POSTGRES_DB=healthetl
ENV POSTGRES_USER=healthetl_user
ENV POSTGRES_PASSWORD=healthetl_password

# Expose PostgreSQL port
EXPOSE 5432 