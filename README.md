
Run:
```
docker compose up --build
```

- Change httpc/hackney in file [src/myapp_sup.erl](src/myapp_sup.erl) (default httpc)
- Change CountParallelRequests in file [src/myapp_sup.erl](src/myapp_sup.erl) (default 100)

Cleanup
```
docker compose down --rmi local -v --remove-orphans
docker rmi caddy:2
```
