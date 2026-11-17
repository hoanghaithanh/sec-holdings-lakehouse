# Deploying to UAT

## How it works

Every push to `main` runs CI, which builds the image and pushes it to GHCR as
`ghcr.io/hoanghaithanh/sec-holdings-lakehouse:sha-<7 chars>`. When CI passes,
[`deploy-uat.yml`](../../.github/workflows/deploy-uat.yml) starts on a self-hosted GitHub Actions
runner that lives on the homelab. GitHub's own runners can't reach the LAN, but a self-hosted
runner connects *out* to GitHub and waits for jobs, so no port or tunnel has to be opened.

The job fast-forwards `~/sec-lakehouse` to the commit, pulls the image pinned to that commit and
runs `docker compose up -d`, then waits for the Dagster webserver to report healthy. If the push
only touched `docs/` or `*.md` files it updates the checkout and skips the restart.

Manual redeploy of the current `main`: Actions tab, **Deploy UAT**, **Run workflow**. Wait until
CI on that commit is green first, or the pinned image won't exist yet.

## One-time setup: install the runner

Run on the homelab as `hoangh` (already in the `docker` group and owns `~/sec-lakehouse`).

1. On GitHub: repo **Settings → Actions → Runners → New self-hosted runner**, Linux x64. The page
   shows download commands and a one-hour registration token.
2. On the server, run the page's download commands in a new folder, for example
   `~/actions-runner-sec-lakehouse`, then configure with the `uat` label the workflow targets:

   ```bash
   ./config.sh --url https://github.com/hoanghaithanh/sec-holdings-lakehouse \
     --token <TOKEN> --name homelab-uat --labels uat --unattended
   sudo ./svc.sh install hoangh   # run as a systemd service under hoangh
   sudo ./svc.sh start
   ```

3. Check it shows **Idle** under Settings → Actions → Runners.

The repo is public, so in **Settings → Actions → General** set fork pull request workflows to
**Require approval for all external contributors**. That stops a stranger's PR from running code
on the homelab runner before you've read it. The runner is in the `docker` group, which is as
good as root on the box, so never point a `pull_request` job at `runs-on: [self-hosted, uat]`.

## Checking and rolling back

- Status: `sudo ./svc.sh status` in the runner folder, or the Actions tab.
- Roll back: re-running an older **Deploy UAT** run does nothing, because the job skips commits
  older than what is deployed (re-running the latest one redeploys it). Revert the bad commit on `main` with a normal PR instead.
  In an emergency on the server: `git checkout <good-sha>` in `~/sec-lakehouse`, then
  `LAKEHOUSE_IMAGE=ghcr.io/hoanghaithanh/sec-holdings-lakehouse:sha-<good7> make up-uat`.
- `make up-uat` pulls the image first. Without that, `:latest` from `.env.uat` resolved to whatever
  stale copy the server had cached, and Dagster came up without the newer assets.
- Deploys stuck in "Queued" after a reboot: the runner may have started before the clock synced
  and quit with "runner registration has been deleted". Check `timedatectl`, then
  `sudo ./svc.sh start` in the runner folder, and cancel and re-run the queued deploy.

## Disk layout: everything heavy lives on `/mnt/md0`

The root SSD is small, so every service that writes a lot keeps its data in a folder under
`$LAKEHOUSE_DATA_DIR` (`/mnt/md0/sec-lakehouse` in `.env.uat`), bind-mounted through a named volume
in `docker-compose.uat.yml`:

| Folder under `$LAKEHOUSE_DATA_DIR` | Used by | Holds |
|---|---|---|
| `postgres/` | Postgres | Dagster runs, events, schedules |
| `object-store/` | RustFS | raw zips, Bronze Delta tables |
| `spark-worker-1/`, `spark-worker-2/` | each Spark worker, at `/spark` | `work/`: each app's jars and executor logs (`SPARK_WORKER_DIR`); `local/`: shuffle and spill files (`SPARK_LOCAL_DIRS`) |

Spark creates `work/` and `local/` itself (the containers run as root). Finished apps' `work/`
folders are deleted a day after the app stops (`spark.worker.cleanup.*`); `local/` is cleaned by
Spark when each app ends.

### One-time setup for the Spark worker folders

Do this on the server **before merging** the PR that adds the worker volumes. The automatic deploy
after the merge is the first thing that needs these folders, and there is no step in between.

```bash
cd ~/sec-lakehouse
D="$(grep '^LAKEHOUSE_DATA_DIR=' .env.uat | cut -d= -f2-)"
# Each step runs only if the one before worked. mkdir has no -p, so it fails unless $D
# already exists, and $D only exists on the mounted array. chmod: executor logs land here.
mountpoint -q /mnt/md0 \
  && mkdir "${D:?}/spark-worker-1" "${D:?}/spark-worker-2" \
  && chmod 700 "$D/spark-worker-1" "$D/spark-worker-2" \
  && echo done || echo "STOPPED: is /mnt/md0 mounted and LAKEHOUSE_DATA_DIR set?"
```

If the folders are missing when the deploy runs, Compose still creates the volumes (the bind
path is only checked when a container starts), the workers fail to start with a mount error,
and the **Deploy UAT** job goes red, possibly after other services were already recreated on the
new image. Create the folders, then re-run the failed **Deploy UAT** run from the Actions tab.

Spark writes these folders as root, so deleting anything under them by hand needs `sudo`.
`local/` is normally emptied when each app ends, but a killed worker or executor can leave
`spark-*` folders behind, so glance at `sudo du -sh "$D"/spark-worker-*/local` now and then.

After the deploy, check that the workers write to the RAID array and not the SSD (with `D` set
as above):

```bash
docker compose -f docker-compose.yml -f docker-compose.uat.yml --env-file .env.uat \
  exec spark-worker-1 sh -c 'df -h /spark && env | grep -E "SPARK_(WORKER_DIR|LOCAL_DIRS)"'
sudo du -sh "$D"/spark-worker-*   # grows while a backfill runs
df -h /                           # should stay flat
```

### If `/mnt/md0` is not mounted at boot

Every data folder above depends on the RAID array being mounted before Docker starts the
containers. If it isn't (a degraded or failed array, a missing `/etc/fstab` entry, or the array
assembling slowly after a reboot), `/mnt/md0` is just an empty folder on the SSD and
`$LAKEHOUSE_DATA_DIR/...` does not exist. What happens then:

- **The containers with a data volume fail to start**: Postgres, RustFS and both Spark workers.
  `docker compose ps -a` shows them as not running, and `journalctl -u docker` shows
  `failed to mount local volume ... no such file or directory`. Dagster's webserver and daemon
  start but can't reach Postgres, so the UI shows errors and no runs launch. This is the safe
  outcome: nothing writes to the SSD by mistake.
- **Docker does not retry them on its own.** At boot Docker tries each `unless-stopped`
  container once. Its restart policy only reacts to a container that started and then exited,
  so a mount failure leaves it down until someone acts.
- **The dangerous case is creating the folders while the array is missing.** If someone runs the
  `mkdir -p` above (or any tool creates the folders) while `/mnt/md0` is unmounted, the folders
  land on the SSD, the services start, and Postgres comes up *empty* on the SSD. When the array is
  mounted later, it hides those folders and the data written in the meantime. That's why the setup
  step checks `mountpoint` first. Never create these folders without that check.

To recover:

1. `mountpoint /mnt/md0` and `cat /proc/mdstat`. If the array is degraded or failed, fix the
   array first; don't start the stack.
2. Mount it (`sudo mount /mnt/md0`, or `sudo mount -a` if it is in `/etc/fstab`), then
   `ls /mnt/md0/sec-lakehouse` should list `postgres`, `object-store` and the worker folders.
3. Start what failed with the image that is already deployed, without pulling a new one:
   `docker compose -f docker-compose.yml -f docker-compose.uat.yml --env-file .env.uat start`.
   (Or re-run the latest **Deploy UAT** run from the Actions tab.) The Dagster webserver and
   daemon reconnect once Postgres is up; they don't need a restart. Then check
   `docker compose ps` and the Dagster UI as after a normal deploy.

Optional hardening (needs `sudo` on the server, so only with Thanh's OK): make Docker wait for
the mount with a systemd drop-in, so a slow array delays the containers instead of failing them.

```bash
sudo systemctl edit docker.service
# add:
# [Unit]
# RequiresMountsFor=/mnt/md0
```

It takes effect at the next boot. It adds `Requires=` and `After=` on the mount, so a `nofail`
option in `/etc/fstab` doesn't weaken it, and Docker waits for the array up to the mount's
device timeout (90 s by default). The trade-off: if the array can't be mounted at all, Docker
itself won't start, which also stops the other Docker apps on the server, such as `sparkpb`.
Decide whether that is worth it before adding it.
