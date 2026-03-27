
Run:
```
docker compose up --build
```

- Change httpc/hackney in file [src/myapp_sup.erl](src/myapp_sup.erl) (default httpc)
- Change CountParallelRequests in file [src/myapp_sup.erl](src/myapp_sup.erl) (default 100)
- Change RequestBodyProfile in file [src/myapp_sup.erl](src/myapp_sup.erl) (`small` | `medium` | `large`, default `large`)
- Change EnableHttpcDebugTrace in file [src/myapp_sup.erl](src/myapp_sup.erl) to enable or disable `dbg` tracing for `httpc`
