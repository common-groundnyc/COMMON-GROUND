# Deploy Dagster Code Containers

Dagster runs in Docker on the Hetzner host, with the repo checked out at `/opt/dagster-pipeline/`. Local dagster-dev is forbidden (macOS forkserver OOMs at 60 GB).

## Procedure

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
cd /opt/dagster-pipeline
git pull
docker compose up -d --build dagster-code
docker compose logs -f dagster-daemon
```

## Verify

Open https://dagster.common-ground.nyc (Cloudflare Access gated). Check the asset count is ~376 and no dagster-code import errors in the logs.
