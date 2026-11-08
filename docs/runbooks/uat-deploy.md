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
