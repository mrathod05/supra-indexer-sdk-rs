# sdk-example

Indexes `token_factory::TokenCreated` from Supra testnet into a `tokens_created` table.

## Run

```bash
cp sdk-example/.env.example sdk-example/.env   # edit DATABASE_URL
createdb supra_indexer_example
cd sdk-example && cargo run
```

Migrations (`migrations/`) run on startup. The SDK creates its own `supra_indexer_checkpoints` table.

```bash
curl localhost:8080/status
curl -i localhost:8080/ready      # 503 while reindexing, 200 once live
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" localhost:8080/admin/pause
```

## Docker

```bash
docker build -f sdk-example/Dockerfile -t supra-indexer-example .
docker run --rm -p 8080:8080 -e DATABASE_URL=postgres://... supra-indexer-example
```

## Adding an event

1. Add a payload struct matching the event's `data` (Move integers are strings).
2. Add a migration and a store implementing `EventStore<SupraEvent<Payload>>`, with an idempotent insert.
3. Call `indexer.register_event::<Payload, _>("module::Event", Store)`. Register events that others depend on first.

See `src/token_created.rs`.
