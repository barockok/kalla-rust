# Kalla Deployment Guide

Kalla is a Universal Reconciliation Engine with a Rust backend (Axum, DataFusion) and a TypeScript/Next.js frontend featuring an agentic AI recipe builder. This guide covers all deployment scenarios from single-node Docker Compose to local development.

---

## 1. Prerequisites

### Docker Deployment

- Docker >= 24.0
- Docker Compose >= 2.20

### Local Development

- Rust >= 1.85
- Node.js >= 22
- PostgreSQL >= 16

### Minimum System Resources

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 4 GB | 8 GB |
| Disk | 10 GB | 20 GB |
| CPU | 2 cores | 4 cores |

Rust compilation is memory-intensive. If you are building inside Docker, ensure your Docker daemon has at least 4 GB of memory allocated. On macOS with Docker Desktop, this is configured under Settings > Resources.

---

## 2. Docker Compose Deployment (Single Node)

### Service Architecture

The `docker-compose.yml` defines four services:

| Service | Image / Build | Port | Purpose |
|---------|---------------|------|---------|
| **postgres** | `postgres:16-alpine` | 5432 | PostgreSQL database |
| **db-init** | Custom (runs `init.sql`) | -- | Schema initialization on startup |
| **server** | Rust/Axum backend build | 3001 | API server and reconciliation engine |
| **web** | Next.js frontend build | 3000 | Web interface |

**Dependency chain:**

```
postgres (healthy)
  -> db-init (completed)
    -> server (healthy)
      -> web
```

- `postgres` starts first and exposes a healthcheck via `pg_isready`.
- `db-init` waits for postgres to be healthy, then runs `init.sql` to create tables and seed data. It exits after completion.
- `server` waits for both postgres to be healthy and db-init to complete successfully before starting.
- `web` waits for the server to be available before starting.

### Deployment Steps

```bash
# Clone the repository
git clone <repo>
cd kalla

# Create environment file from template
cp .env.example .env

# Edit .env with your configuration (see Section 3 for variable reference)
# At minimum, review POSTGRES_PASSWORD and ANTHROPIC_API_KEY
vi .env

# Build and start all services in detached mode
docker compose up -d

# Verify all services are running
docker compose ps
```

Expected output from `docker compose ps` should show:

- `postgres` -- running (healthy)
- `db-init` -- exited (0)
- `server` -- running
- `web` -- running

### Viewing Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f server
```

### Stopping Services

```bash
# Stop all services (preserves data volumes)
docker compose down

# Stop and remove all data (destructive)
docker compose down -v
```

---

## 3. Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POSTGRES_USER` | No | `kalla` | PostgreSQL username |
| `POSTGRES_PASSWORD` | No | `kalla_secret` | PostgreSQL password |
| `POSTGRES_DB` | No | `kalla` | Database name |
| `DATABASE_URL` | Yes (auto) | `postgres://kalla:kalla_secret@postgres:5432/kalla` | Full PostgreSQL connection string. Auto-constructed in Docker from the above variables. |
| `RUST_LOG` | No | `info` | Rust log level. Valid values: `trace`, `debug`, `info`, `warn`, `error` |
| `ANTHROPIC_API_KEY` | No | *(none)* | Anthropic API key for AI recipe generation. Required only if using AI features. |
| `ANTHROPIC_MODEL` | No | *(auto)* | Model override for AI features. When unset, the server selects the default model. |
| `NEXT_PUBLIC_API_URL` | No | `http://localhost:3001` | API URL used by the frontend in the browser (client-side requests). |
| `SERVER_API_URL` | No | `http://server:3001` | API URL used by the frontend on the server side (Next.js SSR). In Docker, this should reference the internal service name. |

**Notes:**

- `DATABASE_URL` is automatically constructed in the Docker Compose configuration from `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`. You typically do not need to set it explicitly unless you are connecting to an external database.
- `NEXT_PUBLIC_API_URL` is a browser-side variable. It must be reachable from the user's browser, not from inside the Docker network.
- `SERVER_API_URL` is used for server-side rendering in Next.js. Inside Docker, it should use the Docker internal hostname (`server`) rather than `localhost`.

---

## 4. Health Check Endpoints

### Server Health

```
GET /health
```

Returns `"OK"` with HTTP status `200` when the server is ready to accept requests.

### PostgreSQL Health

The Docker Compose healthcheck uses:

```bash
pg_isready -U kalla
```

This verifies that PostgreSQL is accepting connections.

### Monitoring Script

A minimal monitoring script for use with cron or an external monitoring system:

```bash
#!/bin/bash

# Check server health
if ! curl -sf http://localhost:3001/health > /dev/null 2>&1; then
    echo "$(date): Server unhealthy" >&2
    # Add alerting logic here (email, Slack webhook, etc.)
    exit 1
fi

# Check PostgreSQL (requires psql or pg_isready on the host)
if ! docker compose exec -T postgres pg_isready -U kalla > /dev/null 2>&1; then
    echo "$(date): PostgreSQL unhealthy" >&2
    exit 1
fi

echo "$(date): All services healthy"
```

---

## 5. Data Persistence

### Docker Volumes

| Volume | Mount Point | Purpose |
|--------|-------------|---------|
| `postgres_data` | `/var/lib/postgresql/data` | PostgreSQL database files |
| `evidence_data` | `/app/evidence` | Reconciliation evidence files |

### Bind Mounts

| Host Path | Container Path | Mode | Purpose |
|-----------|----------------|------|---------|
| `./testdata/` | `/app/testdata` | Read-only | Sample data for testing and demos |

### Resetting Data

To completely reset all data and start fresh:

```bash
docker compose down -v
```

**Warning:** The `-v` flag destroys all named volumes, including `postgres_data` and `evidence_data`. This operation is irreversible.

### Backing Up Data

```bash
# Back up PostgreSQL
docker compose exec -T postgres pg_dump -U kalla kalla > backup_$(date +%Y%m%d).sql

# Back up evidence volume
docker run --rm -v kalla_evidence_data:/data -v $(pwd):/backup alpine \
    tar czf /backup/evidence_backup_$(date +%Y%m%d).tar.gz -C /data .
```

### Restoring Data

```bash
# Restore PostgreSQL
docker compose exec -T postgres psql -U kalla kalla < backup_20250101.sql

# Restore evidence volume
docker run --rm -v kalla_evidence_data:/data -v $(pwd):/backup alpine \
    tar xzf /backup/evidence_backup_20250101.tar.gz -C /data
```

---

## 6. Production Considerations

### Security

- **Change the default PostgreSQL password.** The default `kalla_secret` is for development only. Use a strong, randomly generated password in production.
- **Restrict network access.** Do not expose PostgreSQL port 5432 to the public internet. Remove the port mapping from `docker-compose.yml` or bind it to `127.0.0.1:5432:5432`.
- **Use HTTPS.** Place a reverse proxy (nginx, Caddy, or Traefik) in front of the web and server services to terminate TLS.
- **Protect the API key.** If `ANTHROPIC_API_KEY` is set, ensure the `.env` file has restrictive permissions (`chmod 600 .env`).

### Logging

- Set `RUST_LOG=info` or `RUST_LOG=warn` in production. Avoid `debug` or `trace` as they produce high log volume and may include sensitive data.
- Consider forwarding Docker logs to a centralized logging system (e.g., Loki, Elasticsearch, or CloudWatch).

### Reverse Proxy Example (Caddy)

```
kalla.example.com {
    handle /api/* {
        reverse_proxy server:3001
    }
    handle /health {
        reverse_proxy server:3001
    }
    handle {
        reverse_proxy web:3000
    }
}
```

### Backups

- Back up the `postgres_data` volume regularly. Use `pg_dump` for logical backups or volume snapshots for physical backups.
- Back up the `evidence_data` volume if reconciliation evidence files need to be preserved.
- Store backups off-host (S3, GCS, or remote storage).

### Resource Monitoring

- Monitor disk usage for the `evidence_data` volume. Reconciliation runs can generate significant evidence data over time.
- Monitor PostgreSQL connection count and query performance.
- Set up alerts for the `/health` endpoint (see Section 4).

---

## 7. Local Development Setup

Local development runs PostgreSQL in Docker while running the backend and frontend natively on the host for faster iteration.

### Database

Start only PostgreSQL and run the schema initialization:

```bash
docker compose up -d postgres db-init
```

Wait for db-init to complete:

```bash
docker compose logs -f db-init
# Look for: "db-init exited with code 0"
```

### Backend (Rust/Axum)

```bash
# Set the database connection string (host is localhost, not the Docker service name)
export DATABASE_URL=postgres://kalla:kalla_secret@localhost:5432/kalla

# Optional: enable debug logging during development
export RUST_LOG=debug

# Optional: set Anthropic key for AI features
export ANTHROPIC_API_KEY=sk-ant-...

# Build and run the server
cd kalla-server
cargo run
```

The server will be available at `http://localhost:3001`. The first build may take several minutes due to Rust compilation.

For faster recompilation during development, consider using `cargo-watch`:

```bash
cargo install cargo-watch
cargo watch -x run
```

### Frontend (Next.js)

```bash
cd kalla-web

# Install dependencies
npm install

# Configure API URLs (both point to localhost in local dev)
export NEXT_PUBLIC_API_URL=http://localhost:3001
export SERVER_API_URL=http://localhost:3001

# Start the development server with hot reload
npm run dev
```

The frontend will be available at `http://localhost:3000`.

---

## 8. Troubleshooting

### Server fails to start

**Symptom:** The server container exits immediately or logs connection errors.

**Resolution:**
1. Verify `DATABASE_URL` is correct and the postgres service is healthy:
   ```bash
   docker compose ps postgres
   docker compose logs postgres
   ```
2. Ensure the database credentials in `.env` match what PostgreSQL was initialized with. If you changed credentials after first run, you may need to reset the volume:
   ```bash
   docker compose down -v
   docker compose up -d
   ```

### "relation does not exist"

**Symptom:** The server logs SQL errors like `relation "reconciliations" does not exist`.

**Resolution:** The `db-init` service may not have completed successfully.
```bash
docker compose logs db-init
```
If it shows errors, check the `init.sql` file for syntax issues. To re-run initialization:
```bash
docker compose restart db-init
```

### Frontend cannot reach the API

**Symptom:** The web interface shows network errors or fails to load data.

**Resolution:**
- **Browser-side errors** (visible in browser developer console): Check `NEXT_PUBLIC_API_URL`. This must be reachable from the user's browser. In local development, use `http://localhost:3001`. In production behind a reverse proxy, use the public URL.
- **Server-side errors** (visible in `docker compose logs web`): Check `SERVER_API_URL`. Inside Docker, this should be `http://server:3001`. In local development, use `http://localhost:3001`.

### AI recipe generation fails

**Symptom:** The AI recipe builder returns errors or does not respond.

**Resolution:**
1. Verify `ANTHROPIC_API_KEY` is set in your `.env` file.
2. Check the server logs for API error details:
   ```bash
   docker compose logs server | grep -i anthropic
   ```
3. Ensure your API key is valid and has sufficient quota.

### Docker build runs out of memory

**Symptom:** The build process is killed with signal 9 (OOM) during Rust compilation.

**Resolution:** Rust builds require approximately 2-3 GB of RAM. Increase the Docker daemon memory limit:
- **Docker Desktop (macOS/Windows):** Settings > Resources > Memory -- set to at least 4 GB.
- **Linux:** Check system memory and swap configuration. Consider adding swap if RAM is limited.

### Port conflicts

**Symptom:** `docker compose up` fails with "address already in use" errors.

**Resolution:** Another process is using port 3000 or 3001. Either stop the conflicting process or change the port mappings in `docker-compose.yml`:

```yaml
services:
  server:
    ports:
      - "3002:3001"  # Map to host port 3002 instead
  web:
    ports:
      - "3003:3000"  # Map to host port 3003 instead
```

Remember to update `NEXT_PUBLIC_API_URL` if you change the server port mapping.

---

## 9. Upgrading

### Standard Upgrade

```bash
# Pull latest changes
git pull

# Rebuild containers with new code
docker compose build

# Restart services with new images
docker compose up -d
```

### Database Migrations

Database migrations in Kalla are idempotent. The `init.sql` script uses `CREATE TABLE IF NOT EXISTS` and similar safe patterns, so re-running the initialization on an existing database will not destroy data.

If a release includes schema changes, the `db-init` service will apply them automatically on the next restart:

```bash
docker compose up -d db-init
```

### Verifying the Upgrade

After upgrading, verify that all services are running correctly:

```bash
# Check service status
docker compose ps

# Verify server health
curl -sf http://localhost:3001/health && echo "Server OK"

# Check logs for errors
docker compose logs --tail=50 server
docker compose logs --tail=50 web
```
