---
name: devservices-troubleshooting
description: Fix connection-refused/RPC Unavailable errors in objectstore-service tests by starting devservices (GCS/Bigtable emulators)
---

Some tests require external services (GCS emulator, Bigtable emulator) managed by `devservices`.

**Symptoms of missing services:**
- Connection refused errors
- TCP connect error messages
- `RPC error: status: Unavailable`
- Tests in `objectstore-service` for GCS/Bigtable backends fail

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
