# Understanding Docker images

The following starts an interactive container with a bash prompt.

```bash
docker run -it --entrypoint bash python:3.13
```

Output:
```text
Unable to find image 'python:3.13' locally
3.13: Pulling from library/python
4a1c41792403: Pull complete 
c9b629762372: Pull complete 
3fffeb567ed4: Pull complete 
5582010cab7f: Pull complete 
2470fab23101: Pull complete 
599d5b6b6766: Pull complete 
6a2920e3d16b: Pull complete 
Digest: sha256:c8b03b4e98b39cfb180a5ea13ae5ee39039a8f75ccf52fe6d5c216eed6e1be1d
Status: Downloaded newer image for python:3.13
root@e2192db6d18d:/#
```

Once inside the container, the following is run to get the version of pip.

```bash
pip --version
```

Output:

```text
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

# Understanding Docker networking and docker-compose

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

For the above `docker-compose.yaml` file, the `hostname` and `port` that pgadmin should use to connect to the postgres database are `db:5432`.
