# SLURM Notification

A lightweight Rust daemon that monitors your Slurm jobs and sends a Slack
notification when they finish.

它会在任务进入 `COMPLETED`、`FAILED`、`CANCELLED`、`TIMEOUT`、
`OUT_OF_MEMORY`、`NODE_FAIL` 等结束状态后发送消息。监控器同时使用
`squeue` 和 `sacct`，因此也能发现两次轮询之间已经快速结束的任务。

## Features

- Monitors all jobs belonging to one Slurm user.
- Detects both running jobs and short jobs that finish between polls.
- Sends success and failure details, including exit code, elapsed time,
  partition, nodes, and end time.
- Reports a job array once, as a single summary, after all of its tasks
  have finished.
- Reports the final result of requeued jobs (for example after preemption).
- Deduplicates notifications using a persistent local state file.
- Retries Slack delivery when a request fails.
- Can run in the background after the terminal or SSH session closes.
- Automatically removes monitor log entries older than three days.
- Has no third-party Rust dependencies.

## Requirements

- Linux
- Rust 1.69 or newer
- Slurm commands `squeue` and `sacct`
- `curl`
- Linux utilities `timeout` and `setsid`
- A Slack incoming webhook URL

## Build

```bash
git clone https://github.com/ZYnoe/SLURM-notification.git
cd SLURM-notification
cargo build --release
```

The executable is generated at `target/release/slurm-notify`.

Optionally install it into your user executable directory:

```bash
mkdir -p ~/.local/bin
install -m 755 target/release/slurm-notify ~/.local/bin/slurm-notify
```

## Configure Slack

Create a Slack app with an incoming webhook, then store the webhook in a
private configuration file:

```bash
mkdir -p ~/.config/slurm-notify
cp slack.env.example ~/.config/slurm-notify/slack.env
chmod 600 ~/.config/slurm-notify/slack.env
```

Edit `~/.config/slurm-notify/slack.env` and replace the placeholder:

```text
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK
```

The process also accepts the webhook through an environment variable:

```bash
export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/YOUR/WEBHOOK'
```

Send a test message before starting the monitor:

```bash
./target/release/slurm-notify test-slack
```

Do not commit the real webhook URL. The repository only contains
`slack.env.example`.

## Usage

### Start monitoring

```bash
./target/release/slurm-notify start
```

The command starts one detached background process and returns immediately.
By default it monitors the current user every 30 seconds.

### Check status

```bash
./target/release/slurm-notify status
```

### Follow the log

```bash
tail -f ~/.local/state/slurm-notify/monitor.log
```

The monitor writes every poll and every delivery attempt to this log instead
of printing continuously to the terminal. It removes log entries older than
three days and checks for expired entries once per hour.

### Stop monitoring

```bash
./target/release/slurm-notify stop
```

### Run once

Run one check in the foreground:

```bash
./target/release/slurm-notify once
```

Show the Slack payload without sending it or marking jobs as notified:

```bash
./target/release/slurm-notify once --dry-run
```

### Run in the foreground

```bash
./target/release/slurm-notify foreground
```

This is useful for running the monitor under `tmux`, supervisor, or a
`systemd --user` service.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `--user USER` | Current user | Slurm user to monitor |
| `--interval SECONDS` | `30` | Poll interval |
| `--lookback-hours HOURS` | `6` | `sacct` history window |
| `--state-file PATH` | `~/.local/state/slurm-notify/state.tsv` | Deduplication state |
| `--pid-file PATH` | `~/.local/state/slurm-notify/monitor.pid` | Background process PID |
| `--log-file PATH` | `~/.local/state/slurm-notify/monitor.log` | Background log (3-day retention) |
| `--env-file PATH` | `~/.config/slurm-notify/slack.env` | Webhook configuration |
| `--dry-run` | Disabled | Print messages without sending them |

Example with a 15-second interval:

```bash
./target/release/slurm-notify start --interval 15
```

## How it works

1. `squeue` provides the current set of active job IDs.
2. `sacct` provides recently completed jobs, including short jobs that were
   never observed by `squeue`.
3. Jobs that disappear from `squeue` are queried explicitly by ID so that
   long-running jobs are detected even after they are no longer in the
   configured `sacct` lookback window.
4. Finished job IDs are stored in the state file and are not notified twice.
   Each Slack message is marked as delivered on its own, so a failure part
   way through a batch does not resend the messages that already went out.
5. Finished jobs are grouped into one Slack message per poll when possible.
6. Array tasks (`123_4`) are held until no task of the same array is pending
   or running, then sent as one summary with per-state counts and the tasks
   that did not complete. `squeue --array` lists pending tasks individually;
   pending ranges such as `123_[5-9]` reported by `sacct` are ignored.
7. If a job that was already reported appears in `squeue` again as pending or
   running, it was requeued. The earlier result is discarded and the outcome
   of the new attempt is reported. While `squeue` shows a job as pending or
   running, finished `sacct` rows for it are treated as stale.
8. Unfinished jobs that neither `squeue` nor `sacct` has reported for 7 days
   are dropped from the state file, with a log entry.

On first start, jobs completed during the lookback window are treated as a
baseline and are not sent. Jobs that finish after the monitor starts are
notified normally.

## Runtime files

| File | Purpose |
| --- | --- |
| `~/.local/state/slurm-notify/state.tsv` | Job state and deduplication |
| `~/.local/state/slurm-notify/monitor.log` | Monitor log, retained for three days |
| `~/.local/state/slurm-notify/monitor.pid` | Background process ID |
| `~/.config/slurm-notify/slack.env` | Private Slack webhook configuration |

## Background process lifetime

`start` uses `setsid`, redirects standard input/output/error, and detaches the
monitor from the terminal. Closing the terminal or an SSH session should not
stop it.

The monitor does not survive a login-node reboot. Some clusters also terminate
all user processes when a user logs out. If your cluster enforces that policy,
configure `systemd --user` with lingering or run the monitor on infrastructure
that is intended for persistent user services.

Only one lightweight instance should monitor a given user and state file.
Check the cluster policy before leaving long-running processes on a login
node.

### Clusters with several login nodes

The PID file records the host the monitor was started on. When `status`,
`start`, or `stop` runs on a different login node that shares the same home
directory, the process cannot be checked directly, so the state file, which
the monitor rewrites on every poll, is used as a heartbeat:

- If the state file changed within three poll intervals plus three minutes,
  the monitor is reported as running on the other host. `start` refuses to
  launch a second copy and `stop` tells you which host to log in to.
- If it has not changed for longer than that, the other monitor is treated
  as gone (for example after a reboot) and `start` launches a new one here.

## Troubleshooting

### No Slack test message

Check that the configuration contains your real webhook rather than the
example placeholder:

```bash
grep -q 'REPLACE/ME' ~/.config/slurm-notify/slack.env \
  && echo 'webhook is still a placeholder'
```

Then check network access:

```bash
curl -I https://hooks.slack.com
```

If DNS or TLS fails, run the monitor on a node with outbound HTTPS access or
configure the cluster's proxy environment variables.

### Monitor is not running

```bash
tail -50 ~/.local/state/slurm-notify/monitor.log
```

Also verify that Slurm commands work for your account:

```bash
squeue --user "$USER"
sacct --user "$USER" --starttime now-1hour
```

## Tests

```bash
cargo test --locked
```

The test suite covers Slurm output parsing, Slack payload construction,
notification deduplication, log retention, short jobs, long-running jobs,
job arrays, requeued jobs, partial Slack failures, monitors on another login
node, and background start/stop management.

## License

[MIT](LICENSE)
