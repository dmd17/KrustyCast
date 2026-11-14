//! Runs simulations over a seed range for one or all scenarios.
//!
//!   krusty-sim [--scenario NAME|all] [--seeds START..END]
//!
//! Exits non-zero on any violation and prints the seed, so a failure can be
//! replayed exactly with `--seeds N..N+1`.
use krusty_sim::{SCENARIOS, run, scenario};
use std::time::Instant;

fn main() {
    let mut name = "all".to_string();
    let (mut start, mut end) = (0u64, 100u64);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--scenario" => name = args.next().expect("--scenario NAME"),
            "--seeds" => {
                let r = args.next().expect("--seeds START..END");
                let (a, b) = r.split_once("..").expect("--seeds START..END");
                (start, end) = (a.parse().unwrap(), b.parse().unwrap());
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let names: Vec<&str> = if name == "all" {
        SCENARIOS.to_vec()
    } else {
        vec![name.as_str()]
    };
    let mut failures = 0;
    for n in names {
        let p = scenario(n).unwrap_or_else(|| panic!("unknown scenario {n}"));
        let t = Instant::now();
        let (mut published, mut datagrams) = (0, 0);
        for seed in start..end {
            let r = run(seed, &p);
            published += r.published;
            datagrams += r.datagrams;
            if !r.violations.is_empty() {
                failures += 1;
                println!("FAIL {n} seed {seed}:");
                for v in r.violations.iter().take(10) {
                    println!("  {v}");
                }
            }
        }
        println!(
            "{n:<16} seeds {start}..{end}: {published} messages, {datagrams} datagrams, {:.1} s",
            t.elapsed().as_secs_f64()
        );
    }
    if failures > 0 {
        println!("{failures} failing run(s)");
        std::process::exit(1);
    }
}
