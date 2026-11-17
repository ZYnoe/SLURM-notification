use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture {
    root: PathBuf,
    bin: PathBuf,
    state: PathBuf,
    pid: PathBuf,
    log: PathBuf,
    env_file: PathBuf,
    squeue: PathBuf,
    sacct_recent: PathBuf,
    sacct_jobs: PathBuf,
    curl_count: PathBuf,
    curl_config: PathBuf,
    curl_body: PathBuf,
    curl_all: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = env::temp_dir().join(format!(
            "slurm-notify-test-{}-{unique}",
            std::process::id()
        ));
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();

        write_executable(
            &bin.join("squeue"),
            "#!/bin/sh\ncat \"$FAKE_SQUEUE_FILE\"\n",
        );
        write_executable(
            &bin.join("sacct"),
            "#!/bin/sh\n\
             for arg in \"$@\"; do\n\
               if [ \"$arg\" = \"--jobs\" ]; then\n\
                 cat \"$FAKE_SACCT_JOBS_FILE\"\n\
                 exit 0\n\
               fi\n\
             done\n\
             cat \"$FAKE_SACCT_RECENT_FILE\"\n",
        );
        write_executable(
            &bin.join("curl"),
            "#!/bin/sh\n\
             cat > \"$FAKE_CURL_CONFIG\"\n\
             sent=$(cat \"$FAKE_CURL_COUNT\" 2>/dev/null | wc -c)\n\
             if [ -n \"$FAKE_CURL_FAIL_FROM\" ] && [ \"$sent\" -ge \"$FAKE_CURL_FAIL_FROM\" ]; then\n\
               echo 'curl: (22) simulated failure' >&2\n\
               exit 22\n\
             fi\n\
             payload=$(sed -n 's/^data-binary = \"@\\(.*\\)\"$/\\1/p' \"$FAKE_CURL_CONFIG\")\n\
             if [ -n \"$payload\" ]; then\n\
               cp \"$payload\" \"$FAKE_CURL_BODY\"\n\
               cat \"$payload\" >> \"$FAKE_CURL_ALL\"\n\
               echo >> \"$FAKE_CURL_ALL\"\n\
             fi\n\
             printf x >> \"$FAKE_CURL_COUNT\"\n",
        );

        let fixture = Self {
            state: root.join("state.tsv"),
            pid: root.join("monitor.pid"),
            log: root.join("monitor.log"),
            env_file: root.join("slack.env"),
            squeue: root.join("squeue.out"),
            sacct_recent: root.join("sacct-recent.out"),
            sacct_jobs: root.join("sacct-jobs.out"),
            curl_count: root.join("curl-count"),
            curl_config: root.join("curl-config"),
            curl_body: root.join("curl-body"),
            curl_all: root.join("curl-all"),
            root,
            bin,
        };
        fs::write(
            &fixture.env_file,
            "SLACK_WEBHOOK_URL=https://hooks.slack.com/services/test/test/test\n",
        )
        .unwrap();
        fs::set_permissions(&fixture.env_file, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&fixture.squeue, "200|RUNNING\n").unwrap();
        fs::write(
            &fixture.sacct_recent,
            "100|COMPLETED|0:0|00:00:10|2026-11-16T12:00:00|amd|n001|old-job\n",
        )
        .unwrap();
        fs::write(&fixture.sacct_jobs, "").unwrap();
        fixture
    }

    fn run_once(&self, dry_run: bool) -> Output {
        let mut command = self.command("once");
        if dry_run {
            command.arg("--dry-run");
        }
        command.output().unwrap()
    }

    fn command(&self, mode: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_slurm-notify"));
        command
            .args([mode, "--user", "testuser"])
            .arg("--state-file")
            .arg(&self.state)
            .arg("--pid-file")
            .arg(&self.pid)
            .arg("--log-file")
            .arg(&self.log)
            .arg("--env-file")
            .arg(&self.env_file)
            .env("PATH", format!("{}:{}", self.bin.display(), env::var("PATH").unwrap()))
            .env("FAKE_SQUEUE_FILE", &self.squeue)
            .env("FAKE_SACCT_RECENT_FILE", &self.sacct_recent)
            .env("FAKE_SACCT_JOBS_FILE", &self.sacct_jobs)
            .env("FAKE_CURL_COUNT", &self.curl_count)
            .env("FAKE_CURL_CONFIG", &self.curl_config)
            .env("FAKE_CURL_BODY", &self.curl_body)
            .env("FAKE_CURL_ALL", &self.curl_all)
            .env(
                "SLACK_WEBHOOK_URL",
                "https://hooks.slack.com/services/test/test/test",
            );
        command
    }

    fn run_ok(&self, label: &str) {
        let output = self.run_once(false);
        assert!(
            output.status.success(),
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn set_slurm(&self, squeue: &str, sacct_recent: &str, sacct_jobs: &str) {
        fs::write(&self.squeue, squeue).unwrap();
        fs::write(&self.sacct_recent, sacct_recent).unwrap();
        fs::write(&self.sacct_jobs, sacct_jobs).unwrap();
    }

    fn body(&self) -> String {
        fs::read_to_string(&self.curl_body).unwrap_or_default()
    }

    fn curl_count(&self) -> usize {
        fs::read_to_string(&self.curl_count)
            .map(|content| content.len())
            .unwrap_or(0)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn write_executable(path: &Path, content: &str) {
    fs::write(path, content).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn notifies_new_and_vanished_jobs_once() {
    let fixture = Fixture::new();

    // First run establishes a baseline: old job 100 and active job 200 are not sent.
    let first = fixture.run_once(true);
    assert!(
        first.status.success(),
        "first run failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(fixture.curl_count(), 0);

    // Job 101 starts and finishes between polls. Job 200 started before the
    // accounting lookback window and is found through the vanished-job query.
    fs::write(&fixture.squeue, "").unwrap();
    fs::write(
        &fixture.sacct_recent,
        "100|COMPLETED|0:0|00:00:10|2026-11-16T12:00:00|amd|n001|old-job\n\
         101|FAILED|2:0|00:00:03|2026-11-16T12:01:00|amd|n002|short-job\n",
    )
    .unwrap();
    fs::write(
        &fixture.sacct_jobs,
        "200|COMPLETED|0:0|2-01:02:03|2026-11-16T12:02:00|gpu|n010|long-job\n",
    )
    .unwrap();

    let second = fixture.run_once(false);
    assert!(
        second.status.success(),
        "notification run failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(fixture.curl_count(), 1);
    let config = fs::read_to_string(&fixture.curl_config).unwrap();
    assert!(config.contains("request = \"POST\""));
    assert!(config.contains(
        "url = \"https://hooks.slack.com/services/test/test/test\""
    ));
    let body = fs::read_to_string(&fixture.curl_body).unwrap();
    assert!(body.starts_with("{\"text\":"));
    assert!(body.contains("Job 101"));
    assert!(body.contains("short-job"));
    assert!(body.contains("Job 200"));
    assert!(body.contains("long-job"));

    // Both jobs are now marked as notified in the persistent state.
    let third = fixture.run_once(false);
    assert!(
        third.status.success(),
        "deduplication run failed: {}",
        String::from_utf8_lossy(&third.stderr)
    );
    assert_eq!(fixture.curl_count(), 1);
}

#[test]
fn starts_and_stops_in_the_background() {
    let fixture = Fixture::new();
    let start = fixture.command("start").output().unwrap();
    assert!(
        start.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    let mut running = false;
    for _ in 0..20 {
        let status = fixture.command("status").output().unwrap();
        if String::from_utf8_lossy(&status.stdout).contains("is running") {
            running = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(running, "background monitor did not become healthy");

    let stop = fixture.command("stop").output().unwrap();
    assert!(
        stop.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
    let status = fixture.command("status").output().unwrap();
    assert!(String::from_utf8_lossy(&status.stdout).contains("not running"));
}

#[test]
fn background_monitor_prunes_expired_logs() {
    let fixture = Fixture::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let day = 24 * 60 * 60;
    fs::write(
        &fixture.log,
        format!(
            "[slurm-notify {}] expired entry\n\
             [slurm-notify {}] retained entry\n",
            now - 4 * day,
            now - day
        ),
    )
    .unwrap();

    let start = fixture.command("start").output().unwrap();
    assert!(
        start.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    let mut pruned = false;
    for _ in 0..40 {
        let log = fs::read_to_string(&fixture.log).unwrap_or_default();
        if !log.contains("expired entry") && log.contains("retained entry") {
            pruned = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(pruned, "background monitor did not prune expired log entry");

    let stop = fixture.command("stop").output().unwrap();
    assert!(
        stop.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
}

fn row(id: &str, state: &str, exit: &str, name: &str) -> String {
    format!("{id}|{state}|{exit}|00:10:00|2026-11-16T12:00:00|amd|n001|{name}\n")
}

#[test]
fn array_tasks_are_sent_together_once_the_array_finishes() {
    let fixture = Fixture::new();
    fixture.set_slurm("", &row("100", "COMPLETED", "0:0", "old-job"), "");
    fixture.run_ok("baseline");

    // Task 0 is already done, the others are still running or pending.
    // sacct also reports the pending range, which must be ignored.
    fixture.set_slurm(
        "300_1|RUNNING\n300_2|PENDING\n300_3|PENDING\n",
        &format!(
            "{}{}",
            row("300_0", "COMPLETED", "0:0", "arr"),
            row("300_[2-3]", "PENDING", "0:0", "arr")
        ),
        "",
    );
    fixture.run_ok("partial array");
    assert_eq!(fixture.curl_count(), 0, "array was reported before it finished");

    let done = format!(
        "{}{}{}{}",
        row("300_0", "COMPLETED", "0:0", "arr"),
        row("300_1", "COMPLETED", "0:0", "arr"),
        row("300_2", "FAILED", "1:0", "arr"),
        row("300_3", "COMPLETED", "0:0", "arr")
    );
    fixture.set_slurm("", &done, &done);
    fixture.run_ok("finished array");
    assert_eq!(fixture.curl_count(), 1);
    let body = fixture.body();
    assert!(body.contains("*Array 300*"), "{body}");
    assert!(body.contains("COMPLETED ×3, FAILED ×1"), "{body}");
    assert!(body.contains("`300_2` FAILED"), "{body}");

    let state = fs::read_to_string(&fixture.state).unwrap();
    assert!(!state.contains('['), "array range stored in state:\n{state}");

    fixture.run_ok("dedup");
    assert_eq!(fixture.curl_count(), 1);
}

#[test]
fn requeued_job_reports_its_final_result() {
    let fixture = Fixture::new();
    fixture.set_slurm("400|RUNNING\n", "", "");
    fixture.run_ok("baseline");

    fixture.set_slurm("", "", &row("400", "PREEMPTED", "0:0", "req-job"));
    fixture.run_ok("preempted");
    assert_eq!(fixture.curl_count(), 1);
    assert!(fixture.body().contains("PREEMPTED"));

    // Requeued: squeue shows it again while sacct still has the old row.
    let stale = row("400", "PREEMPTED", "0:0", "req-job");
    fixture.set_slurm("400|PENDING\n", &stale, "");
    fixture.run_ok("requeued pending");
    fixture.set_slurm("400|RUNNING\n", &stale, "");
    fixture.run_ok("requeued running");
    assert_eq!(fixture.curl_count(), 1, "stale PREEMPTED row was sent again");

    let done = row("400", "COMPLETED", "0:0", "req-job");
    fixture.set_slurm("", &done, &done);
    fixture.run_ok("completed");
    assert_eq!(fixture.curl_count(), 2);
    assert!(fixture.body().contains("COMPLETED"));

    // A job that lingers in COMPLETING must not trigger the requeue path.
    fixture.set_slurm("400|COMPLETING\n", &done, &done);
    fixture.run_ok("completing");
    assert_eq!(fixture.curl_count(), 2);
}

#[test]
fn finishing_while_squeue_still_shows_running_is_reported_next_poll() {
    let fixture = Fixture::new();
    fixture.set_slurm("500|RUNNING\n", "", "");
    fixture.run_ok("baseline");

    // The job ends between the squeue and sacct calls of one poll.
    let done = row("500", "COMPLETED", "0:0", "race-job");
    fixture.set_slurm("500|RUNNING\n", &done, "");
    fixture.run_ok("race");
    assert_eq!(fixture.curl_count(), 0);

    fixture.set_slurm("", &done, &done);
    fixture.run_ok("next poll");
    assert_eq!(fixture.curl_count(), 1);
    fixture.run_ok("dedup");
    assert_eq!(fixture.curl_count(), 1);
}

#[test]
fn partial_slack_failure_does_not_resend_delivered_messages() {
    let fixture = Fixture::new();
    fixture.set_slurm("", "", "");
    fixture.run_ok("baseline");

    let name = "a-long-job-name-for-message-splitting".repeat(3);
    let rows: String = (600..640)
        .map(|id| row(&id.to_string(), "COMPLETED", "0:0", &name))
        .collect();
    fixture.set_slurm("", &rows, "");

    let first = fixture.command("once").env("FAKE_CURL_FAIL_FROM", "1").output().unwrap();
    assert!(first.status.success());
    assert_eq!(fixture.curl_count(), 1, "only the first message should succeed");

    fixture.run_ok("retry");
    fixture.run_ok("dedup");
    let all = fs::read_to_string(&fixture.curl_all).unwrap();
    for id in 600..640 {
        let marker = format!("*Job {id}*");
        assert_eq!(all.matches(&marker).count(), 1, "{marker} not sent exactly once");
    }
}

#[test]
fn respects_a_monitor_running_on_another_login_node() {
    let fixture = Fixture::new();
    fixture.run_ok("create state");
    fs::write(&fixture.pid, "999999\nsome-other-login-node\n30\n").unwrap();

    let status = fixture.command("status").output().unwrap();
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(stdout.contains("some-other-login-node"), "{stdout}");

    let start = fixture.command("start").output().unwrap();
    assert!(!start.status.success(), "second monitor started on another host");
    assert!(String::from_utf8_lossy(&start.stderr).contains("some-other-login-node"));
    let stop = fixture.command("stop").output().unwrap();
    assert!(!stop.status.success());
    assert!(fixture.pid.exists(), "PID file of the other host was removed");

    // Once the shared state file stops changing, the old monitor is treated as gone.
    let touched = Command::new("touch")
        .args(["-d", "1 hour ago"])
        .arg(&fixture.state)
        .status()
        .unwrap();
    assert!(touched.success());
    let status = fixture.command("status").output().unwrap();
    assert!(String::from_utf8_lossy(&status.stdout).contains("not running"));

    let start = fixture.command("start").output().unwrap();
    assert!(
        start.status.success(),
        "start after stale heartbeat failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let pid_file = fs::read_to_string(&fixture.pid).unwrap();
    assert_eq!(pid_file.lines().count(), 3, "{pid_file}");
    let stop = fixture.command("stop").output().unwrap();
    assert!(
        stop.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
}
