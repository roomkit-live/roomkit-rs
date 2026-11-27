//! What every measurement records: the machine, the commit, and the other
//! load on the measured cores, pass by pass, as the Python implementation's
//! `benchmarks/media` does.

use std::error::Error;
use std::process::Command;

use serde_json::{Value, json};
use tokio::time::Instant;

/// Other processes may take this share of one measured core during a pass;
/// past it the pass is run again.
const MAX_OTHER_LOAD: f64 = 0.10;
/// How many times a pass spoiled by other load is run again.
const RETRIES: usize = 2;
/// The kernel's clock ticks per second in `/proc` (`USER_HZ`, 100 on Linux).
const TICKS_PER_SECOND: f64 = 100.0;

/// A pass's samples: each case's, in seconds, in the order the pass ran them.
pub(crate) type Samples = Vec<(String, Vec<f64>)>;

/// One pass: its samples by case, the wall time it took, the CPU time other
/// processes spent on the measured cores meanwhile, as a share of one core,
/// and how many attempts it took.
#[derive(Debug)]
pub(crate) struct Pass {
    pub(crate) samples: Samples,
    pub(crate) seconds: f64,
    pub(crate) other_load: f64,
    pub(crate) attempts: usize,
}

impl Pass {
    pub(crate) fn to_json(&self) -> Value {
        let samples: serde_json::Map<String, Value> = self
            .samples
            .iter()
            .map(|(case, values)| (case.clone(), json!(values)))
            .collect();
        json!({
            "seconds": self.seconds,
            "other_load": self.other_load,
            "attempts": self.attempts,
            "samples": samples,
        })
    }
}

/// Runs `measure` once, again while other processes spoil it (up to
/// [`RETRIES`] more times); the last attempt is kept either way.
pub(crate) fn run_pass(
    measure: impl Fn() -> Result<Samples, Box<dyn Error>>,
) -> Result<Pass, Box<dyn Error>> {
    let cpus = allowed_cpus();
    let mut attempts = 0;
    loop {
        attempts += 1;
        let (busy, own) = (busy_seconds(&cpus), own_seconds());
        let began = Instant::now();
        let samples = measure()?;
        let seconds = began.elapsed().as_secs_f64();
        let others = busy_seconds(&cpus) - busy - (own_seconds() - own);
        let other_load = if seconds > 0.0 {
            others.max(0.0) / seconds
        } else {
            0.0
        };
        if other_load <= MAX_OTHER_LOAD || attempts > RETRIES {
            return Ok(Pass {
                samples,
                seconds,
                other_load,
                attempts,
            });
        }
    }
}

/// The nearest-rank p50, p99 and max of a case's samples.
#[derive(Debug)]
pub(crate) struct Summary {
    pub(crate) n: usize,
    pub(crate) p50: f64,
    pub(crate) p99: f64,
    pub(crate) max: f64,
}

pub(crate) fn summary(values: &[f64]) -> Summary {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let rank = |q: f64| {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "a rank among a few hundred samples"
        )]
        let at = ((q * ordered.len() as f64).ceil() as usize).max(1);
        ordered.get(at - 1).copied().unwrap_or(f64::NAN)
    };
    Summary {
        n: ordered.len(),
        p50: rank(0.5),
        p99: rank(0.99),
        max: rank(1.0),
    }
}

fn command(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// The date, the machine, the commit and the toolchain of a measurement.
pub(crate) fn environment() -> Value {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = cpuinfo
        .lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split_once(':'))
        .map(|(_, model)| model.trim().to_owned());
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|text| text.trim().to_owned())
        .ok();
    let dirty = command("git", &["status", "--porcelain", "--untracked-files=no"])
        .is_some_and(|status| !status.is_empty());
    json!({
        "date": iso_date(unix_seconds()),
        "cpu_model": model,
        "kernel": kernel,
        "cpus": allowed_cpus(),
        "commit": command("git", &["rev-parse", "HEAD"]),
        "dirty": dirty,
        "rustc": command("rustc", &["--version"]),
    })
}

/// A name for a run's directory: the date and time, in UTC.
pub(crate) fn stamp() -> String {
    iso_date(unix_seconds()).replace([':', '-'], "")
}

#[expect(
    clippy::disallowed_methods,
    reason = "a measurement is dated on the real clock"
)]
fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// `seconds` since the epoch as an ISO 8601 UTC date and time.
fn iso_date(seconds: u64) -> String {
    let days = seconds / 86_400;
    let (hour, minute, second) = (seconds % 86_400 / 3600, seconds % 3600 / 60, seconds % 60);
    // Days to a civil date (H. Hinnant's algorithm), from 1970-01-01.
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}+00:00")
}

/// The CPUs this process may run on (`taskset`), from `/proc/self/status`.
fn allowed_cpus() -> Vec<usize> {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let list = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .unwrap_or_default();
    let mut cpus = Vec::new();
    for part in list.trim().split(',').filter(|part| !part.is_empty()) {
        let (low, high) = part.split_once('-').unwrap_or((part, part));
        if let (Ok(low), Ok(high)) = (low.parse::<usize>(), high.parse::<usize>()) {
            cpus.extend(low..=high);
        }
    }
    cpus
}

fn ticks(fields: &[&str], at: usize) -> f64 {
    fields
        .get(at)
        .and_then(|value| value.parse::<u32>().ok())
        .map_or(0.0, f64::from)
}

/// User, nice, system and steal time spent on `cpus` since boot, as
/// `/proc/stat` counts it: interrupts are left out, as they are not charged
/// to the processes they serve.
fn busy_seconds(cpus: &[usize]) -> f64 {
    let stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let mut total = 0.0;
    for line in stat.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(cpu) = fields
            .first()
            .and_then(|name| name.strip_prefix("cpu"))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            continue;
        };
        if cpus.contains(&cpu) {
            total += ticks(&fields, 1) + ticks(&fields, 2) + ticks(&fields, 3) + ticks(&fields, 8);
        }
    }
    total / TICKS_PER_SECOND
}

/// This process's user and system time, every thread, from `/proc/self/stat`.
fn own_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let after = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
    let fields: Vec<&str> = after.split_whitespace().collect();
    (ticks(&fields, 11) + ticks(&fields, 12)) / TICKS_PER_SECOND
}
