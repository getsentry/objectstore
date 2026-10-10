---
name: devservices-troubleshooting
description: Fix connection-refused/RPC Unavailable errors in objectstore-service tests by starting devservices (GCS/Bigtable emulators, Cassandra)
---

Some tests require external services (GCS emulator, Bigtable emulator, Cassandra) managed by `devservices`.

**Symptoms of missing services:**
- Connection refused errors
- TCP connect error messages
- `RPC error: status: Unavailable`
- `Failed to find a node with working connection pool` (CQL backend)
- Tests in `objectstore-service` for GCS/Bigtable/CQL backends fail

**How to fix:**

1. Check devservices status:
   ```bash
   devservices status
   ```

2. Start devservices if not running:
   ```bash
   devservices up --mode=full
   ```

3. Devservices run in the background - you only need to start them once per session

4. Cassandra takes up to a minute to start; wait until `devservices status` reports it healthy
