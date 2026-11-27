//! Measurements of RoomKit on the real clock, run by hand, never by the
//! gates: `cargo run --release -p roomkit-benchmarks -- <kind>` (see
//! `benchmarks/README.md`).
//!
//! - `close`: the kit's close against its bound (RFC §12.10.4,
//!   RK-12.10.4-056), on the Python implementation's cases.

#![expect(clippy::print_stdout, reason = "a measurement prints its summary")]

mod close;
mod machine;

use std::error::Error;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use machine::{Pass, environment, run_pass, summary};

const USAGE: &str = "usage: roomkit-benchmarks close [--passes N] [--output DIR]";

struct Args {
    passes: usize,
    output: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("close") {
        return Err(USAGE.to_owned());
    }
    let mut parsed = Args {
        passes: 3,
        output: None,
    };
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| USAGE.to_owned())?;
        match flag.as_str() {
            "--passes" => {
                parsed.passes = value.parse().map_err(|_| USAGE.to_owned())?;
                if parsed.passes == 0 {
                    return Err("--passes must be at least 1".to_owned());
                }
            }
            "--output" => parsed.output = Some(PathBuf::from(value)),
            _ => return Err(USAGE.to_owned()),
        }
    }
    Ok(parsed)
}

/// The summary of a run of `close`, and whether every case passed.
fn report(env: &serde_json::Value, passes: &[Pass]) -> (String, bool) {
    let mut text = String::new();
    let mut ok = true;
    let field = |name: &str| env.get(name).map_or(String::new(), ToString::to_string);
    let _ = writeln!(text, "# Measurement: close\n");
    let _ = writeln!(text, "- date: {}", field("date"));
    let _ = writeln!(
        text,
        "- machine: {}, kernel {}, CPUs {}",
        field("cpu_model"),
        field("kernel"),
        field("cpus")
    );
    let _ = writeln!(
        text,
        "- commit: {} (dirty: {})",
        field("commit"),
        field("dirty")
    );
    let _ = writeln!(text, "- toolchain: {}", field("rustc"));
    let loads: Vec<String> = passes
        .iter()
        .map(|pass| {
            format!(
                "{:.1} s, other load {:.1} % of a core{}",
                pass.seconds,
                100.0 * pass.other_load,
                if pass.attempts > 1 {
                    format!(", {} attempts", pass.attempts)
                } else {
                    String::new()
                }
            )
        })
        .collect();
    let _ = writeln!(text, "- passes: {}\n", loads.join(", "));
    let _ = writeln!(
        text,
        "| Case | n | p50 (s) | p99 (s) | max (s) | Past the grace (s) | Bound (s) | Verdict |"
    );
    let _ = writeln!(text, "|---|---:|---:|---:|---:|---:|---:|---|");
    let grace = close::GRACE.as_secs_f64();
    for (case, _) in close::CASES {
        let values: Vec<f64> = passes
            .iter()
            .filter_map(|pass| pass.samples.iter().find(|(name, _)| name == case))
            .flat_map(|(_, values)| values.iter().copied())
            .collect();
        let s = summary(&values);
        let bound = close::bound(case).as_secs_f64();
        let past = (s.max - grace).max(0.0);
        // Within the RFC's sum of waits, the close's own work besides, and
        // work that honours its cut spends no unwind.
        let within = s.max <= bound + close::OWN_WORK.as_secs_f64()
            && (case.ends_with("-ignores") || past < close::UNWIND.as_secs_f64());
        ok &= within;
        let _ = writeln!(
            text,
            "| {case} | {} | {:.3} | {:.3} | {:.3} | {past:.3} | {bound:.2} | {} |",
            s.n,
            s.p50,
            s.p99,
            s.max,
            if within { "pass" } else { "FAIL" }
        );
    }
    let _ = writeln!(
        text,
        "\nA case passes within its bound plus {} ms for the close's own work between \
         its waits, and, for work that honours its cut, with no unwind spent.",
        close::OWN_WORK.as_millis()
    );
    (text, ok)
}

fn run() -> Result<ExitCode, Box<dyn Error>> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(usage) => {
            println!("{usage}");
            return Ok(ExitCode::from(2));
        }
    };
    let env = environment();
    let output = args.output.unwrap_or_else(|| {
        PathBuf::from("target/benchmark-results").join(format!("close-{}", machine::stamp()))
    });
    std::fs::create_dir_all(output.parent().unwrap_or(&output))?;
    std::fs::create_dir(&output)?;
    let mut passes = Vec::with_capacity(args.passes);
    for _ in 0..args.passes {
        passes.push(run_pass(close::pass)?);
    }
    let (text, ok) = report(&env, &passes);
    let results = serde_json::json!({
        "kind": "close",
        "environment": env,
        "verdict": ok,
        "passes": passes.iter().map(Pass::to_json).collect::<Vec<_>>(),
    });
    std::fs::write(
        output.join("results.json"),
        serde_json::to_string_pretty(&results)?,
    )?;
    std::fs::write(output.join("summary.md"), &text)?;
    println!("{text}\nresults: {}", output.display());
    Ok(if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            println!("roomkit-benchmarks: {error}");
            ExitCode::FAILURE
        }
    }
}
