use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const STATE_VERSION: &str = "1";
const TERMINAL_STATES: &[&str] = &[
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
    "SPECIAL_EXIT",
];
const SACCT_FORMAT: &str =
    "JobID,State,ExitCode,Elapsed,End,Partition,NodeList,JobName%120";
const MAX_SLACK_TEXT_CHARS: usize = 3500;
const LOG_RETENTION_SECS: u64 = 3 * 24 * 60 * 60;
const LOG_PRUNE_INTERVAL_SECS: u64 = 60 * 60;
const FINISHED_RETENTION_SECS: u64 = 90 * 24 * 60 * 60;
/// Unfinished jobs that neither squeue nor sacct has reported for this long
/// are dropped so that they are not queried forever.
const UNFINISHED_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
/// Extra time, on top of three poll intervals, before a monitor on another
/// login node is considered gone because the shared state file stopped changing.
const HEARTBEAT_GRACE_SECS: u64 = 180;
const MAX_LISTED_ARRAY_PROBLEMS: usize = 10;

#[derive(Clone, Debug)]
struct Config {
    user: String,
    interval_secs: u64,
    lookback_hours: u64,
    state_file: PathBuf,
    pid_file: PathBuf,
    log_file: PathBuf,
    env_file: PathBuf,
    dry_run: bool,
}

impl Config {
    fn defaults() -> Result<Self, String> {
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| "HOME is not set".to_string())?;
        let data_home = env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/state"));
        let config_home = env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        let data_dir = data_home.join("slurm-notify");
        let user = env::var("USER")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(current_username);

        Ok(Self {
            user,
            interval_secs: 30,
            lookback_hours: 6,
            state_file: data_dir.join("state.tsv"),
            pid_file: data_dir.join("monitor.pid"),
            log_file: data_dir.join("monitor.log"),
            env_file: config_home.join("slurm-notify/slack.env"),
            dry_run: false,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct JobRecord {
    id: String,
    name: String,
    state: String,
    exit_code: String,
    elapsed: String,
    end: String,
    partition: String,
    node_list: String,
    terminal: bool,
    notified: bool,
    last_seen: u64,
}

impl JobRecord {
    fn from_accounting(line: &str, now: u64) -> Option<Self> {
        let fields: Vec<&str> = line.splitn(8, '|').collect();
        if fields.len() != 8 || fields[0].trim().is_empty() {
            return None;
        }
        let state = clean(&fields[1]);
        Some(Self {
            id: clean(&fields[0]),
            name: clean(&fields[7]),
            terminal: is_terminal(&state),
            state,
            exit_code: clean(&fields[2]),
            elapsed: clean(&fields[3]),
            end: clean(&fields[4]),
            partition: clean(&fields[5]),
            node_list: clean(&fields[6]),
            notified: false,
            last_seen: now,
        })
    }

    fn mark_running(&mut self, now: u64) {
        self.last_seen = now;
        if !self.terminal {
            self.state = "RUNNING_OR_PENDING".to_string();
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MonitorState {
    initialized: bool,
    jobs: BTreeMap<String, JobRecord>,
}

impl MonitorState {
    fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = fs::read_to_string(path)
            .map_err(|error| format!("cannot read state file {}: {error}", path.display()))?;
        let mut state = Self::default();
        for (line_number, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.splitn(12, '\t').collect();
            match fields.first().copied() {
                Some("VERSION") => {
                    if fields.get(1).copied() != Some(STATE_VERSION) {
                        return Err(format!(
                            "unsupported state version on line {}",
                            line_number + 1
                        ));
                    }
                }
                Some("INITIALIZED") => state.initialized = fields.get(1) == Some(&"1"),
                Some("JOB") if fields.len() == 12 => {
                    let record = JobRecord {
                        id: fields[1].to_string(),
                        name: fields[2].to_string(),
                        state: fields[3].to_string(),
                        exit_code: fields[4].to_string(),
                        elapsed: fields[5].to_string(),
                        end: fields[6].to_string(),
                        partition: fields[7].to_string(),
                        node_list: fields[8].to_string(),
                        terminal: fields[9] == "1",
                        notified: fields[10] == "1",
                        last_seen: fields[11].parse().unwrap_or(0),
                    };
                    if !record.id.is_empty() {
                        state.jobs.insert(record.id.clone(), record);
                    }
                }
                _ => {
                    return Err(format!(
                        "invalid state data on line {}",
                        line_number + 1
                    ));
                }
            }
        }
        Ok(state)
    }

    fn save(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!("cannot create state directory {}: {error}", parent.display())
            })?;
        }

        let mut output = String::new();
        output.push_str(&format!("VERSION\t{STATE_VERSION}\n"));
        output.push_str(&format!(
            "INITIALIZED\t{}\n",
            if self.initialized { 1 } else { 0 }
        ));
        for record in self.jobs.values() {
            output.push_str(&format!(
                "JOB\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                field(&record.id),
                field(&record.name),
                field(&record.state),
                field(&record.exit_code),
                field(&record.elapsed),
                field(&record.end),
                field(&record.partition),
                field(&record.node_list),
                if record.terminal { 1 } else { 0 },
                if record.notified { 1 } else { 0 },
                record.last_seen
            ));
        }

        let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)
                .map_err(|error| {
                    format!(
                        "cannot create temporary state file {}: {error}",
                        temporary.display()
                    )
                })?;
            file.write_all(output.as_bytes()).map_err(|error| {
                format!(
                    "cannot write temporary state file {}: {error}",
                    temporary.display()
                )
            })?;
            file.sync_all().map_err(|error| {
                format!(
                    "cannot sync temporary state file {}: {error}",
                    temporary.display()
                )
            })?;
        }
        fs::rename(&temporary, path).map_err(|error| {
            let _ = fs::remove_file(&temporary);
            format!("cannot replace state file {}: {error}", path.display())
        })
    }

    /// Removes old records and returns the IDs of unfinished jobs that were
    /// dropped because Slurm stopped reporting them.
    fn prune(&mut self, now: u64) -> Vec<String> {
        let mut expired = Vec::new();
        self.jobs.retain(|id, record| {
            // Pending array ranges such as `123_[4-9]` are not real job IDs and
            // could never be resolved; older versions stored them.
            if id.contains('[') {
                return false;
            }
            let age = now.saturating_sub(record.last_seen);
            if record.terminal {
                !record.notified || age <= FINISHED_RETENTION_SECS
            } else if age <= UNFINISHED_RETENTION_SECS {
                true
            } else {
                expired.push(id.clone());
                false
            }
        });
        expired
    }
}

fn main() {
    if let Err(error) = run() {
        log(&format!("error: {error}"));
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut raw_args: Vec<String> = env::args().skip(1).collect();
    let mode = match raw_args.first().map(String::as_str) {
        Some("start" | "stop" | "status" | "once" | "foreground" | "test-slack") => {
            raw_args.remove(0)
        }
        Some("-h" | "--help" | "help") => {
            print_help();
            return Ok(());
        }
        Some(value) if value.starts_with('-') => "foreground".to_string(),
        Some(value) => return Err(format!("unknown command: {value}")),
        None => "foreground".to_string(),
    };

    let config = parse_config(raw_args)?;
    match mode.as_str() {
        "start" => start(&config),
        "stop" => stop(&config),
        "status" => status(&config),
        "once" => monitor(&config, true),
        "foreground" => monitor(&config, false),
        "test-slack" => test_slack(&config),
        _ => unreachable!(),
    }
}

fn parse_config(args: Vec<String>) -> Result<Config, String> {
    let mut config = Config::defaults()?;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = |offset: usize| -> Result<&str, String> {
            args.get(index + offset)
                .map(String::as_str)
                .ok_or_else(|| format!("missing value for {flag}"))
        };
        match flag.as_str() {
            "--user" => {
                config.user = value(1)?.to_string();
                index += 2;
            }
            "--interval" => {
                config.interval_secs = parse_positive(value(1)?, "--interval")?;
                index += 2;
            }
            "--lookback-hours" => {
                config.lookback_hours = parse_positive(value(1)?, "--lookback-hours")?;
                index += 2;
            }
            "--state-file" => {
                config.state_file = PathBuf::from(value(1)?);
                index += 2;
            }
            "--pid-file" => {
                config.pid_file = PathBuf::from(value(1)?);
                index += 2;
            }
            "--log-file" => {
                config.log_file = PathBuf::from(value(1)?);
                index += 2;
            }
            "--env-file" => {
                config.env_file = PathBuf::from(value(1)?);
                index += 2;
            }
            "--dry-run" => {
                config.dry_run = true;
                index += 1;
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ => return Err(format!("unknown option: {flag}")),
        }
    }
    if config.interval_secs == 0 {
        return Err("--interval must be greater than zero".to_string());
    }
    Ok(config)
}

fn parse_positive(value: &str, name: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(parsed)
}

fn monitor(config: &Config, once: bool) -> Result<(), String> {
    let webhook = if config.dry_run {
        None
    } else {
        Some(load_webhook(&config.env_file)?)
    };
    let mut state = MonitorState::load(&config.state_file)?;
    let first_run = !state.initialized;
    log(&format!(
        "monitor starting: user={}, interval={}s, lookback={}h, first_run={}, dry_run={}",
        config.user, config.interval_secs, config.lookback_hours, first_run, config.dry_run
    ));

    if first_run {
        establish_baseline(config, &mut state)?;
    }

    let mut next_log_prune = unix_now();
    loop {
        let now = unix_now();
        if !once && now >= next_log_prune {
            match prune_monitor_log(&config.log_file, now) {
                Ok(0) => {}
                Ok(lines) => log(&format!(
                    "removed {lines} log line(s) older than {} days",
                    LOG_RETENTION_SECS / (24 * 60 * 60)
                )),
                Err(error) => log(&format!("log cleanup failed: {error}")),
            }
            next_log_prune = now.saturating_add(LOG_PRUNE_INTERVAL_SECS);
        }
        if let Err(error) = poll_once(config, &mut state, webhook.as_deref()) {
            log(&format!("poll failed: {error}"));
        }
        if once {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(config.interval_secs));
    }
}

fn establish_baseline(config: &Config, state: &mut MonitorState) -> Result<(), String> {
    let now = unix_now();
    let active = match squeue_jobs(config) {
        Ok(jobs) => {
            log(&format!("baseline: {} active jobs", jobs.len()));
            jobs.into_keys().collect::<BTreeSet<_>>()
        }
        Err(error) => {
            log(&format!(
                "baseline squeue failed; existing accounting jobs will be used: {error}"
            ));
            BTreeSet::new()
        }
    };
    let recent = sacct_recent(config).map_err(|error| {
        format!("cannot establish notification baseline from sacct: {error}")
    })?;

    for id in active {
        let record = state.jobs.entry(id.clone()).or_default();
        record.id = id;
        record.mark_running(now);
    }
    for record in recent {
        let was_terminal = record.terminal;
        let mut record = record;
        // Existing finished jobs are deliberately not sent on the first run.
        record.notified = was_terminal;
        state.jobs.insert(record.id.clone(), record);
    }
    state.initialized = true;
    log_expired(&state.prune(now));
    state.save(&config.state_file)?;
    log("baseline saved; notifications are active for new job completions");
    Ok(())
}

fn poll_once(config: &Config, state: &mut MonitorState, webhook: Option<&str>) -> Result<(), String> {
    let now = unix_now();
    let mut errors = Vec::new();

    // Job ID -> squeue state. When squeue fails, fall back to the jobs that
    // were unfinished last time, with an unknown state.
    let active: BTreeMap<String, String> = match squeue_jobs(config) {
        Ok(jobs) => jobs,
        Err(error) => {
            errors.push(format!("squeue: {error}"));
            state
                .jobs
                .values()
                .filter(|record| !record.terminal)
                .map(|record| (record.id.clone(), String::new()))
                .collect()
        }
    };
    for (id, squeue_state) in &active {
        let record = state.jobs.entry(id.clone()).or_default();
        record.id = id.clone();
        if record.terminal && is_running_like(squeue_state) {
            // A requeued job keeps its ID. Forget the earlier result so the
            // outcome of the new attempt is reported.
            log(&format!(
                "job {id} is {squeue_state} again after {}; waiting for the new result",
                record.state
            ));
            record.terminal = false;
            record.notified = false;
        }
        record.mark_running(now);
    }

    let mut accounting_records = BTreeMap::<String, JobRecord>::new();
    match sacct_recent(config) {
        Ok(records) => {
            for record in records {
                accounting_records.insert(record.id.clone(), record);
            }
        }
        Err(error) => errors.push(format!("sacct recent: {error}")),
    }

    let vanished: Vec<String> = state
        .jobs
        .values()
        .filter(|record| !record.terminal && !active.contains_key(&record.id))
        .map(|record| record.id.clone())
        .collect();
    for chunk in vanished.chunks(100) {
        match sacct_jobs(config, chunk) {
            Ok(records) => {
                for record in records {
                    accounting_records.insert(record.id.clone(), record);
                }
            }
            Err(error) => errors.push(format!("sacct vanished jobs: {error}")),
        }
    }

    for record in accounting_records.into_values() {
        if record.terminal {
            if active
                .get(&record.id)
                .map_or(false, |squeue_state| is_running_like(squeue_state))
            {
                // squeue still shows the job pending or running, so this
                // accounting row belongs to an earlier attempt or is ahead of
                // squeue. The job is checked again once squeue drops it.
                continue;
            }
            match state.jobs.get_mut(&record.id) {
                Some(existing) if existing.notified => {
                    existing.last_seen = now;
                    if existing.name.is_empty() {
                        existing.name = record.name;
                    }
                }
                _ => {
                    state.jobs.insert(record.id.clone(), record);
                }
            }
        } else {
            let existing = state.jobs.entry(record.id.clone()).or_default();
            if !existing.terminal {
                *existing = record;
            }
        }
    }

    log_expired(&state.prune(now));
    state.save(&config.state_file)?;

    // Array tasks are reported together once every task of the array is done.
    let unfinished_arrays: BTreeSet<String> = state
        .jobs
        .values()
        .filter(|record| !record.terminal)
        .filter_map(|record| array_parent(&record.id).map(ToOwned::to_owned))
        .collect();
    let mut held = 0;
    let pending: Vec<JobRecord> = state
        .jobs
        .values()
        .filter(|record| record.terminal && !record.notified)
        .filter(|record| {
            let hold = array_parent(&record.id)
                .map_or(false, |parent| unfinished_arrays.contains(parent));
            if hold {
                held += 1;
            }
            !hold
        })
        .cloned()
        .collect();
    if !pending.is_empty() {
        let batches = build_slack_messages(&config.user, &pending);
        if let Some(webhook) = webhook {
            for batch in &batches {
                match send_slack(webhook, &batch.text) {
                    Ok(()) => {
                        // Mark each delivered message right away so that a
                        // later failure does not resend it.
                        for id in &batch.job_ids {
                            if let Some(saved) = state.jobs.get_mut(id) {
                                saved.notified = true;
                            }
                        }
                        state.save(&config.state_file)?;
                        log(&format!(
                            "sent Slack notification for {} job(s)",
                            batch.job_ids.len()
                        ));
                    }
                    Err(error) => {
                        errors.push(format!("Slack: {error}"));
                        break;
                    }
                }
            }
        } else {
            for batch in batches {
                log(&format!("dry-run Slack payload:\n{}", batch.text));
            }
        }
    }

    if errors.is_empty() {
        let held_note = if held > 0 {
            format!(", {held} array task(s) waiting for the rest of their array")
        } else {
            String::new()
        };
        log(&format!(
            "poll complete: {} active, {} awaiting notification{held_note}",
            active.len(),
            pending.len()
        ));
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Returns active job IDs with their squeue state. `--array` lists every
/// pending array task on its own line instead of a range like `123_[4-9]`.
fn squeue_jobs(config: &Config) -> Result<BTreeMap<String, String>, String> {
    let output = run_with_timeout(
        "squeue",
        &[
            "--noheader",
            "--array",
            "--user",
            &config.user,
            "--format=%i|%T",
        ],
    )?;
    ensure_success("squeue", &output)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_squeue_output(&stdout))
}

fn parse_squeue_output(stdout: &str) -> BTreeMap<String, String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            let (id, state) = line.split_once('|').unwrap_or((line, ""));
            let id = id.trim();
            if id.is_empty() || id.contains('[') {
                None
            } else {
                Some((id.to_string(), state.trim().to_string()))
            }
        })
        .collect()
}

fn sacct_recent(config: &Config) -> Result<Vec<JobRecord>, String> {
    let start = format!("now-{}hours", config.lookback_hours);
    let output = run_with_timeout(
        "sacct",
        &[
            "--noheader",
            "--parsable2",
            "--allocations",
            "--user",
            &config.user,
            "--starttime",
            &start,
            "--format",
            SACCT_FORMAT,
        ],
    )?;
    parse_sacct_output("sacct", &output)
}

fn sacct_jobs(config: &Config, job_ids: &[String]) -> Result<Vec<JobRecord>, String> {
    if job_ids.is_empty() {
        return Ok(Vec::new());
    }
    let jobs = job_ids.join(",");
    let output = run_with_timeout(
        "sacct",
        &[
            "--noheader",
            "--parsable2",
            "--allocations",
            "--user",
            &config.user,
            "--jobs",
            &jobs,
            "--format",
            SACCT_FORMAT,
        ],
    )?;
    parse_sacct_output("sacct", &output)
}

fn parse_sacct_output(command: &str, output: &Output) -> Result<Vec<JobRecord>, String> {
    ensure_success(command, output)?;
    let now = unix_now();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut records = Vec::new();
    for line in stdout.lines() {
        if let Some(record) = JobRecord::from_accounting(line, now) {
            // Rows such as `123_[4-9]` stand for array tasks that have not
            // started; each task is tracked individually through squeue.
            if !record.id.contains('[') {
                records.push(record);
            }
        } else if !line.trim().is_empty() {
            log(&format!("ignored malformed sacct row: {line}"));
        }
    }
    Ok(records)
}

fn run_with_timeout(program: &str, args: &[&str]) -> Result<Output, String> {
    Command::new("timeout")
        .arg("45")
        .arg(program)
        .args(args)
        .output()
        .map_err(|error| format!("cannot execute {program}: {error}"))
}

fn ensure_success(command: &str, output: &Output) -> Result<(), String> {
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let detail = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    Err(format!(
        "{command} exited with {}: {}",
        output.status, detail
    ))
}

fn load_webhook(path: &Path) -> Result<String, String> {
    if let Ok(value) = env::var("SLACK_WEBHOOK_URL") {
        if !value.trim().is_empty() {
            return validate_webhook(value.trim(), "SLACK_WEBHOOK_URL");
        }
    }

    let content = fs::read_to_string(path).map_err(|error| {
        format!(
            "SLACK_WEBHOOK_URL is not set and cannot read {}: {error}",
            path.display()
        )
    })?;
    if let Ok(metadata) = fs::metadata(path) {
        if metadata.permissions().mode() & 0o077 != 0 {
            log(&format!(
                "WARNING: {} is accessible by other users; run chmod 600 on it",
                path.display()
            ));
        }
    }
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, raw_value)) = line.split_once('=') {
            if key.trim() == "SLACK_WEBHOOK_URL" {
                let value = raw_value.trim().trim_matches('"').trim_matches('\'');
                if !value.is_empty() {
                    return validate_webhook(value, &path.display().to_string());
                }
            }
        }
    }
    Err(format!(
        "SLACK_WEBHOOK_URL was not found in {}",
        path.display()
    ))
}

fn validate_webhook(value: &str, source: &str) -> Result<String, String> {
    if value.contains("REPLACE/ME") || value.contains("hooks.slack.com/services/...") {
        return Err(format!(
            "{source} still contains the example placeholder; replace it with the real Slack webhook URL"
        ));
    }
    if !value.starts_with("https://") {
        return Err(format!(
            "{source} must contain an https:// Slack webhook URL"
        ));
    }
    Ok(value.to_string())
}

fn send_slack(webhook: &str, message: &str) -> Result<(), String> {
    let body = format!("{{\"text\":\"{}\"}}", json_escape(message));
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let payload_path = env::temp_dir().join(format!(
        "slurm-notify-{}-{}.json",
        std::process::id(),
        unique
    ));
    {
        let mut payload = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&payload_path)
            .map_err(|error| {
                format!(
                    "cannot create temporary Slack payload {}: {error}",
                    payload_path.display()
                )
            })?;
        payload.write_all(body.as_bytes()).map_err(|error| {
            let _ = fs::remove_file(&payload_path);
            format!("cannot write temporary Slack payload: {error}")
        })?;
    }

    let config = format!(
        "silent\nshow-error\nfail\nmax-time = 20\nrequest = \"POST\"\n\
         header = \"Content-type: application/json\"\n\
         data-binary = \"@{}\"\nurl = \"{}\"\n",
        curl_config_escape(&payload_path.display().to_string()),
        curl_config_escape(webhook)
    );
    let mut child = Command::new("curl")
        .args(["--config", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            let _ = fs::remove_file(&payload_path);
            format!("cannot execute curl: {error}")
        })?;
    let write_result = child
        .stdin
        .take()
        .ok_or_else(|| "cannot open curl stdin".to_string())
        .and_then(|mut stdin| {
            stdin
                .write_all(config.as_bytes())
                .map_err(|error| format!("cannot send configuration to curl: {error}"))
        });
    if let Err(error) = write_result {
        let _ = child.kill();
        let _ = child.wait();
        let _ = fs::remove_file(&payload_path);
        return Err(error);
    }
    let output = child
        .wait_with_output()
        .map_err(|error| format!("cannot wait for curl: {error}"));
    let _ = fs::remove_file(&payload_path);
    ensure_success("curl", &output?)
}

fn curl_config_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[derive(Clone, Debug)]
struct SlackBatch {
    text: String,
    job_ids: Vec<String>,
}

fn build_slack_messages(user: &str, jobs: &[JobRecord]) -> Vec<SlackBatch> {
    let mut arrays: BTreeMap<&str, Vec<&JobRecord>> = BTreeMap::new();
    for job in jobs {
        if let Some(parent) = array_parent(&job.id) {
            arrays.entry(parent).or_default().push(job);
        }
    }

    let mut entries: Vec<(String, Vec<String>)> = Vec::new();
    let mut emitted = BTreeSet::new();
    for job in jobs {
        match array_parent(&job.id) {
            Some(parent) if arrays[parent].len() > 1 => {
                if emitted.insert(parent) {
                    let tasks = &arrays[parent];
                    entries.push((
                        format_array(parent, tasks),
                        tasks.iter().map(|task| task.id.clone()).collect(),
                    ));
                }
            }
            _ => entries.push((format_job(job), vec![job.id.clone()])),
        }
    }

    let mut batches = Vec::new();
    let mut body = String::new();
    let mut job_ids = Vec::new();
    for (entry, entry_ids) in entries {
        if !body.is_empty()
            && body.chars().count() + entry.chars().count() + 80 > MAX_SLACK_TEXT_CHARS
        {
            batches.push(SlackBatch {
                text: slack_message(user, &body),
                job_ids: std::mem::take(&mut job_ids),
            });
            body.clear();
        }
        if !body.is_empty() {
            body.push_str("\n\n");
        }
        body.push_str(&entry);
        job_ids.extend(entry_ids);
    }
    if !body.is_empty() {
        batches.push(SlackBatch {
            text: slack_message(user, &body),
            job_ids,
        });
    }
    batches
}

fn slack_message(user: &str, body: &str) -> String {
    format!(":bell: *SLURM 任务结束通知*\n用户: `{}`\n\n{}", user, body)
}

fn format_job(job: &JobRecord) -> String {
    let icon = if job.state.starts_with("COMPLETED") {
        ":white_check_mark:"
    } else if job.state.starts_with("CANCELLED") {
        ":warning:"
    } else {
        ":x:"
    };
    let name = if job.name.is_empty() {
        "(unknown)".to_string()
    } else {
        job.name.replace('`', "'")
    };
    let mut lines = vec![format!(
        "{icon} *Job {}* `{}`",
        job.id, name
    )];
    lines.push(format!("状态: *{}*", job.state));
    if !job.exit_code.is_empty() {
        lines.push(format!("退出码: `{}`", job.exit_code));
    }
    if !job.elapsed.is_empty() {
        lines.push(format!("运行时间: `{}`", job.elapsed));
    }
    if !job.partition.is_empty() {
        lines.push(format!("分区: `{}`", job.partition));
    }
    if !job.node_list.is_empty() {
        lines.push(format!("节点: `{}`", job.node_list));
    }
    if !job.end.is_empty() {
        lines.push(format!("结束时间: `{}`", job.end));
    }
    lines.join("\n")
}

fn format_array(parent: &str, tasks: &[&JobRecord]) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for task in tasks {
        *counts.entry(base_state(&task.state)).or_default() += 1;
    }
    let icon = if counts.keys().all(|state| state == "COMPLETED") {
        ":white_check_mark:"
    } else if counts
        .keys()
        .all(|state| state == "COMPLETED" || state == "CANCELLED")
    {
        ":warning:"
    } else {
        ":x:"
    };
    let name = tasks
        .iter()
        .find(|task| !task.name.is_empty())
        .map(|task| task.name.replace('`', "'"))
        .unwrap_or_else(|| "(unknown)".to_string());
    let summary = counts
        .iter()
        .map(|(state, count)| format!("{state} ×{count}"))
        .collect::<Vec<_>>()
        .join(", ");

    let mut lines = vec![
        format!("{icon} *Array {parent}* `{name}`"),
        format!("任务数: `{}`", tasks.len()),
        format!("状态: *{summary}*"),
    ];

    let problems: Vec<&&JobRecord> = tasks
        .iter()
        .filter(|task| base_state(&task.state) != "COMPLETED")
        .collect();
    if !problems.is_empty() {
        lines.push("未成功的任务:".to_string());
        for task in problems.iter().take(MAX_LISTED_ARRAY_PROBLEMS) {
            let exit = if task.exit_code.is_empty() {
                String::new()
            } else {
                format!(" 退出码 `{}`", task.exit_code)
            };
            lines.push(format!("• `{}` {}{exit}", task.id, task.state));
        }
        if problems.len() > MAX_LISTED_ARRAY_PROBLEMS {
            lines.push(format!(
                "• …另有 {} 个",
                problems.len() - MAX_LISTED_ARRAY_PROBLEMS
            ));
        }
    }

    let partitions: BTreeSet<&str> = tasks
        .iter()
        .map(|task| task.partition.as_str())
        .filter(|partition| !partition.is_empty())
        .collect();
    if partitions.len() == 1 {
        lines.push(format!("分区: `{}`", partitions.iter().next().unwrap()));
    }
    if let Some(end) = tasks
        .iter()
        .map(|task| task.end.as_str())
        .filter(|end| !end.is_empty() && *end != "Unknown")
        .max()
    {
        lines.push(format!("最后结束时间: `{end}`"));
    }
    lines.join("\n")
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c <= '\u{1f}' => escaped.push_str(&format!("\\u{:04x}", c as u32)),
            c => escaped.push(c),
        }
    }
    escaped
}

fn test_slack(config: &Config) -> Result<(), String> {
    let webhook = load_webhook(&config.env_file)?;
    let message = format!(
        ":wave: SLURM 通知测试成功\n用户: `{}`\n监控器已经可以连接 Slack。",
        config.user
    );
    send_slack(&webhook, &message)?;
    println!("Slack test message sent.");
    Ok(())
}

fn start(config: &Config) -> Result<(), String> {
    match locate_monitor(config) {
        MonitorLocation::Local(pid) => {
            return Err(format!(
                "monitor is already running with PID {pid}; see {}",
                config.log_file.display()
            ));
        }
        MonitorLocation::Remote {
            pid,
            host,
            heartbeat_age,
        } => {
            return Err(format!(
                "monitor is already running on {host} (PID {pid}, last poll {heartbeat_age}s ago); \
                 log in to {host} to stop it, or wait until it has been silent for a few minutes"
            ));
        }
        MonitorLocation::StaleRemote { host, heartbeat_age } => {
            println!(
                "Previous monitor on {host} has not polled for {}; starting a new one here.",
                describe_age(heartbeat_age)
            );
            let _ = fs::remove_file(&config.pid_file);
        }
        MonitorLocation::NotRunning => {}
    }
    // Validate the webhook before leaving a broken process in the background.
    if !config.dry_run {
        let _ = load_webhook(&config.env_file)?;
    }
    if let Some(parent) = config.log_file.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!("cannot create log directory {}: {error}", parent.display())
        })?;
    }
    if let Some(parent) = config.pid_file.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!("cannot create PID directory {}: {error}", parent.display())
        })?;
    }

    let executable = env::current_exe()
        .map_err(|error| format!("cannot locate current executable: {error}"))?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&config.log_file)
        .map_err(|error| {
            format!(
                "cannot open log file {}: {error}",
                config.log_file.display()
            )
        })?;
    let error_log = log
        .try_clone()
        .map_err(|error| format!("cannot duplicate log handle: {error}"))?;

    let mut command = Command::new("setsid");
    command
        .arg(executable)
        .arg("foreground")
        .args(config_to_args(config))
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(error_log));
    let child = command
        .spawn()
        .map_err(|error| format!("cannot start background monitor: {error}"))?;
    let pid_content = format!(
        "{}\n{}\n{}\n",
        child.id(),
        current_hostname(),
        config.interval_secs
    );
    if let Err(error) = fs::write(&config.pid_file, pid_content) {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status();
        return Err(format!(
            "cannot write PID file {}: {error}",
            config.pid_file.display()
        ));
    }
    let _ = fs::set_permissions(&config.pid_file, fs::Permissions::from_mode(0o600));
    println!(
        "Monitor started with PID {}. Log: {}",
        child.id(),
        config.log_file.display()
    );
    Ok(())
}

fn stop(config: &Config) -> Result<(), String> {
    let pid = match locate_monitor(config) {
        MonitorLocation::Local(pid) => pid,
        MonitorLocation::Remote { pid, host, .. } => {
            return Err(format!(
                "monitor runs on {host} (PID {pid}); log in to {host} and run stop there"
            ));
        }
        MonitorLocation::StaleRemote { .. } | MonitorLocation::NotRunning => {
            println!("Monitor is not running.");
            return Ok(());
        }
    };

    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map_err(|error| format!("cannot execute kill: {error}"))?;
    if !status.success() {
        return Err(format!("cannot stop monitor with PID {pid}"));
    }
    for _ in 0..50 {
        if !process_alive(pid) {
            let _ = fs::remove_file(&config.pid_file);
            println!("Monitor stopped.");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(200));
    }
    Err(format!(
        "monitor PID {pid} did not stop after 10 seconds"
    ))
}

fn status(config: &Config) -> Result<(), String> {
    match locate_monitor(config) {
        MonitorLocation::Local(pid) => {
            println!("Monitor is running with PID {pid}.");
            println!("Log: {}", config.log_file.display());
            println!("State: {}", config.state_file.display());
        }
        MonitorLocation::Remote {
            pid,
            host,
            heartbeat_age,
        } => {
            println!("Monitor is running on another host: {host} (PID {pid}).");
            println!("Last poll: {heartbeat_age}s ago.");
            println!("Log: {}", config.log_file.display());
        }
        MonitorLocation::StaleRemote { host, heartbeat_age } => {
            println!(
                "Monitor is not running (last seen on {host}, no poll for {}).",
                describe_age(heartbeat_age)
            );
        }
        MonitorLocation::NotRunning => println!("Monitor is not running."),
    }
    Ok(())
}

fn config_to_args(config: &Config) -> Vec<String> {
    let mut args = vec![
        "--user".to_string(),
        config.user.clone(),
        "--interval".to_string(),
        config.interval_secs.to_string(),
        "--lookback-hours".to_string(),
        config.lookback_hours.to_string(),
        "--state-file".to_string(),
        config.state_file.display().to_string(),
        "--pid-file".to_string(),
        config.pid_file.display().to_string(),
        "--log-file".to_string(),
        config.log_file.display().to_string(),
        "--env-file".to_string(),
        config.env_file.display().to_string(),
    ];
    if config.dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

struct PidInfo {
    pid: u32,
    host: Option<String>,
    interval_secs: Option<u64>,
}

enum MonitorLocation {
    NotRunning,
    Local(u32),
    /// Started on another login node that shares this home directory. The
    /// process cannot be checked from here, so the state file, which the
    /// monitor rewrites on every poll, serves as its heartbeat.
    Remote {
        pid: u32,
        host: String,
        heartbeat_age: u64,
    },
    StaleRemote {
        host: String,
        heartbeat_age: Option<u64>,
    },
}

fn locate_monitor(config: &Config) -> MonitorLocation {
    let info = match read_pid(&config.pid_file) {
        Ok(Some(info)) => info,
        _ => return MonitorLocation::NotRunning,
    };
    if let Some(host) = info.host {
        if host != current_hostname() {
            let interval = info.interval_secs.unwrap_or(config.interval_secs);
            let limit = interval
                .saturating_mul(3)
                .saturating_add(HEARTBEAT_GRACE_SECS);
            let heartbeat_age = file_age_secs(&config.state_file);
            return match heartbeat_age {
                Some(age) if age <= limit => MonitorLocation::Remote {
                    pid: info.pid,
                    host,
                    heartbeat_age: age,
                },
                _ => MonitorLocation::StaleRemote {
                    host,
                    heartbeat_age,
                },
            };
        }
    }
    if process_alive(info.pid) && looks_like_monitor(info.pid) {
        MonitorLocation::Local(info.pid)
    } else {
        let _ = fs::remove_file(&config.pid_file);
        MonitorLocation::NotRunning
    }
}

/// Reads `PID`, then optionally `HOSTNAME` and `INTERVAL` on the next lines.
/// PID files from older versions contain only the PID.
fn read_pid(path: &Path) -> Result<Option<PidInfo>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)
        .map_err(|error| format!("cannot read PID file {}: {error}", path.display()))?;
    let mut lines = content.lines().map(str::trim);
    let pid = lines
        .next()
        .unwrap_or("")
        .parse::<u32>()
        .map_err(|_| format!("invalid PID in {}", path.display()))?;
    let host = lines
        .next()
        .filter(|host| !host.is_empty())
        .map(ToOwned::to_owned);
    let interval_secs = lines.next().and_then(|value| value.parse().ok());
    Ok(Some(PidInfo {
        pid,
        host,
        interval_secs,
    }))
}

fn file_age_secs(path: &Path) -> Option<u64> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(
        SystemTime::now()
            .duration_since(modified)
            .map(|age| age.as_secs())
            .unwrap_or(0),
    )
}

fn describe_age(age: Option<u64>) -> String {
    match age {
        Some(age) => format!("{age}s"),
        None => "an unknown time (state file missing)".to_string(),
    }
}

fn current_hostname() -> String {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .or_else(|| {
            Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
                .filter(|name| !name.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn looks_like_monitor(pid: u32) -> bool {
    let executable_name = env::current_exe()
        .ok()
        .and_then(|path| path.file_name().map(|name| name.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "slurm-notify".to_string());
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|command| {
            let command = String::from_utf8_lossy(&command);
            command.contains(&executable_name) && command.contains("foreground")
        })
        .unwrap_or(false)
}

fn current_username() -> String {
    Command::new("id")
        .arg("-un")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn field(value: &str) -> String {
    clean(value)
}

fn clean(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|character| match character {
            '\t' | '\n' | '\r' => ' ',
            other => other,
        })
        .collect()
}

/// `CANCELLED by 1000` -> `CANCELLED`, `COMPLETED+` -> `COMPLETED`.
fn base_state(state: &str) -> String {
    state
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches('+')
        .to_string()
}

fn is_terminal(state: &str) -> bool {
    TERMINAL_STATES.contains(&base_state(state).as_str())
}

/// True when squeue reports the job as waiting or running (as opposed to
/// finishing up or unknown), which is when a finished accounting row for the
/// same ID must be stale.
fn is_running_like(squeue_state: &str) -> bool {
    let state = base_state(squeue_state);
    !state.is_empty()
        && !is_terminal(&state)
        && !matches!(state.as_str(), "COMPLETING" | "STAGE_OUT" | "SIGNALING")
}

/// `123_4` -> `123`. Only array tasks contain an underscore.
fn array_parent(id: &str) -> Option<&str> {
    id.split_once('_').map(|(parent, _)| parent)
}

fn log_expired(expired: &[String]) {
    if !expired.is_empty() {
        log(&format!(
            "dropped {} unfinished job(s) not reported by Slurm for {} days: {}",
            expired.len(),
            UNFINISHED_RETENTION_SECS / (24 * 60 * 60),
            expired.join(",")
        ));
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn log_entry_timestamp(line: &str) -> Option<u64> {
    let rest = line.strip_prefix("[slurm-notify ")?;
    let end = rest.find(']')?;
    rest[..end].parse().ok()
}

fn prune_monitor_log(path: &Path, now: u64) -> Result<usize, String> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(format!(
                "cannot read log file {}: {error}",
                path.display()
            ))
        }
    };
    let cutoff = now.saturating_sub(LOG_RETENTION_SECS);
    let mut retained = String::with_capacity(contents.len());
    let mut current_entry_timestamp = None;
    let mut removed_lines = 0;

    for line in contents.split_inclusive('\n') {
        if let Some(timestamp) = log_entry_timestamp(line) {
            current_entry_timestamp = Some(timestamp);
        }
        let keep = current_entry_timestamp.map_or(true, |timestamp| timestamp >= cutoff);
        if keep {
            retained.push_str(line);
        } else {
            removed_lines += 1;
        }
    }

    if removed_lines == 0 {
        return Ok(0);
    }

    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| format!("cannot open log file {}: {error}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.write_all(retained.as_bytes()))
        .and_then(|_| file.set_len(retained.len() as u64))
        .and_then(|_| file.sync_data())
        .map_err(|error| format!("cannot rewrite log file {}: {error}", path.display()))?;
    Ok(removed_lines)
}

fn log(message: &str) {
    eprintln!("[slurm-notify {}] {}", unix_now(), message);
}

fn print_help() {
    println!(
        "\
slurm-notify - send Slack notifications when Slurm jobs finish

Usage:
  slurm-notify start [OPTIONS]       Start in the background
  slurm-notify stop [OPTIONS]        Stop the background monitor
  slurm-notify status [OPTIONS]      Show monitor status
  slurm-notify once [OPTIONS]        Run one check and exit
  slurm-notify foreground [OPTIONS]  Run in the foreground
  slurm-notify test-slack [OPTIONS]  Send a Slack test message

Options:
  --user USER               Slurm user to monitor (default: current user)
  --interval SECONDS        Poll interval (default: 30)
  --lookback-hours HOURS    sacct history window (default: 6)
  --state-file PATH         State file for deduplication
  --pid-file PATH           Background process PID file
  --log-file PATH           Background monitor log (3-day retention)
  --env-file PATH           File containing SLACK_WEBHOOK_URL
  --dry-run                 Print Slack payloads without sending them
  -h, --help                Show this help

The webhook is read from SLACK_WEBHOOK_URL first, then from the env file.
On first start, existing completed jobs are marked as baseline and are not sent."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sacct_job_name_with_delimiter() {
        let record = JobRecord::from_accounting(
            "123|COMPLETED|0:0|00:01:02|2026-11-16T12:00:00|amd|n001|a|b",
            10,
        )
        .unwrap();
        assert_eq!(record.id, "123");
        assert_eq!(record.name, "a|b");
        assert!(record.terminal);
    }

    #[test]
    fn recognizes_cancelled_state_with_reason() {
        assert!(is_terminal("CANCELLED by 1000"));
        assert!(is_terminal("COMPLETED+"));
        assert!(!is_terminal("RUNNING"));
        assert!(!is_terminal("PENDING"));
    }

    #[test]
    fn escapes_json_control_characters() {
        assert_eq!(
            json_escape("a\"b\\c\n\t\u{0001}"),
            "a\\\"b\\\\c\\n\\t\\u0001"
        );
    }

    #[test]
    fn rejects_example_webhook_placeholder() {
        let error = validate_webhook(
            "https://hooks.slack.com/services/REPLACE/ME",
            "test config",
        )
        .unwrap_err();
        assert!(error.contains("example placeholder"));
    }

    #[test]
    fn builds_multiple_messages_for_large_batches() {
        let jobs: Vec<JobRecord> = (0..100)
            .map(|index| JobRecord {
                id: index.to_string(),
                name: "a-long-job-name-for-message-splitting".repeat(3),
                state: "COMPLETED".to_string(),
                terminal: true,
                ..JobRecord::default()
            })
            .collect();
        let messages = build_slack_messages("zeyu", &jobs);
        assert!(messages.len() > 1);
        assert!(messages
            .iter()
            .all(|message| message.text.chars().count() <= MAX_SLACK_TEXT_CHARS + 100));
        let ids: usize = messages.iter().map(|message| message.job_ids.len()).sum();
        assert_eq!(ids, 100);
    }

    #[test]
    fn parses_squeue_states_and_skips_array_ranges() {
        let jobs = parse_squeue_output("300_1|RUNNING\n300_[2-3]|PENDING\n400|COMPLETING\n500\n");
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs["300_1"], "RUNNING");
        assert_eq!(jobs["400"], "COMPLETING");
        assert_eq!(jobs["500"], "");
    }

    #[test]
    fn running_like_excludes_completing_and_unknown() {
        assert!(is_running_like("PENDING"));
        assert!(is_running_like("RUNNING"));
        assert!(is_running_like("REQUEUED"));
        assert!(!is_running_like("COMPLETING"));
        assert!(!is_running_like("COMPLETED"));
        assert!(!is_running_like(""));
    }

    #[test]
    fn prune_expires_unfinished_jobs_slurm_no_longer_reports() {
        let now = 100 * 24 * 60 * 60;
        let record = |id: &str, terminal: bool, notified: bool, age_days: u64| JobRecord {
            id: id.to_string(),
            terminal,
            notified,
            last_seen: now - age_days * 24 * 60 * 60,
            ..JobRecord::default()
        };
        let mut state = MonitorState::default();
        for job in [
            record("1", false, false, 8),
            record("2", false, false, 1),
            record("3", true, false, 95),
            record("4", true, true, 95),
            record("5_[1-3]", false, false, 0),
        ] {
            state.jobs.insert(job.id.clone(), job);
        }
        let expired = state.prune(now);
        assert_eq!(expired, vec!["1".to_string()]);
        let kept: Vec<&str> = state.jobs.keys().map(String::as_str).collect();
        assert_eq!(kept, vec!["2", "3"]);
    }

    #[test]
    fn groups_array_tasks_into_one_entry() {
        let job = |id: &str, state: &str, exit: &str| JobRecord {
            id: id.to_string(),
            name: "relax".to_string(),
            state: state.to_string(),
            exit_code: exit.to_string(),
            partition: "amd".to_string(),
            terminal: true,
            ..JobRecord::default()
        };
        let jobs = vec![
            job("300_0", "COMPLETED", "0:0"),
            job("300_1", "FAILED", "1:0"),
            job("300_2", "COMPLETED", "0:0"),
            job("301_0", "COMPLETED", "0:0"),
            job("400", "COMPLETED", "0:0"),
        ];
        let batches = build_slack_messages("zeyu", &jobs);
        assert_eq!(batches.len(), 1);
        let text = &batches[0].text;
        assert!(text.contains("*Array 300*"));
        assert!(text.contains("COMPLETED ×2, FAILED ×1"));
        assert!(text.contains("`300_1` FAILED"));
        assert!(!text.contains("`300_0` COMPLETED"));
        // A single finished task is shown like a normal job.
        assert!(text.contains("*Job 301_0*"));
        assert!(text.contains("*Job 400*"));
        assert_eq!(batches[0].job_ids.len(), 5);
    }

    #[test]
    fn reads_pid_files_with_and_without_host() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("slurm-notify-pid-{}-{unique}", std::process::id()));
        fs::write(&path, "42\n").unwrap();
        let old = read_pid(&path).unwrap().unwrap();
        assert_eq!(old.pid, 42);
        assert!(old.host.is_none());
        fs::write(&path, "42\nlogin02\n60\n").unwrap();
        let new = read_pid(&path).unwrap().unwrap();
        assert_eq!(new.host.as_deref(), Some("login02"));
        assert_eq!(new.interval_secs, Some(60));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn prunes_log_entries_older_than_three_days() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = env::temp_dir().join(format!(
            "slurm-notify-log-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("monitor.log");
        let now = 10 * 24 * 60 * 60;
        let old = now - 4 * 24 * 60 * 60;
        let recent = now - 2 * 24 * 60 * 60;
        fs::write(
            &path,
            format!(
                "[slurm-notify {old}] old entry\ncontinued detail\n\
                 [slurm-notify {recent}] recent entry\nrecent detail\n"
            ),
        )
        .unwrap();

        let removed = prune_monitor_log(&path, now).unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(removed, 2);
        assert!(!contents.contains("old entry"));
        assert!(!contents.contains("continued detail"));
        assert!(contents.contains("recent entry"));
        assert!(contents.contains("recent detail"));
        let _ = fs::remove_dir_all(root);
    }
}
