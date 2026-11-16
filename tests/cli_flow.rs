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
             payload=$(sed -n 's/^data-binary = \"@\\(.*\\)\"$/\\1/p' \"$FAKE_CURL_CONFIG\")\n\
             if [ -n \"$payload\" ]; then cp \"$payload\" \"$FAKE_CURL_BODY\"; fi\n\
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
            root,
            bin,
        };
        fs::write(
            &fixture.env_file,
            "SLACK_WEBHOOK_URL=https://hooks.slack.com/services/test/test/test\n",
        )
        .unwrap();
        fs::set_permissions(&fixture.env_file, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&fixture.squeue, "200\n").unwrap();
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
            .env(
                "SLACK_WEBHOOK_URL",
                "https://hooks.slack.com/services/test/test/test",
            );
        command
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
