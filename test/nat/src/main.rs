#![feature(box_syntax)]
extern crate e2d2;
extern crate fnv;
extern crate getopts;
extern crate rand;
extern crate time;

use e2d2::config::{basic_opts, read_matches};
use e2d2::interface::*;
use e2d2::operators::*;
use e2d2::scheduler::*;
use self::nf::*;
use std::env;
use std::fmt::Display;
use std::net::Ipv4Addr;
use std::process;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod nf;

const CONVERSION_FACTOR: f64 = 1000000000.;

fn test<T, S>(ports: Vec<T>, sched: &mut S)
    where
        T: PacketRx + PacketTx + Display + Clone + 'static,
        S: Scheduler + Sized,
{
    println!("Receiving started");

    let mut pipelines: Vec<_> = ports
        .iter()
        .map(|port| {
            ReceiveBatch::new(port.clone())
                .filter(box |_| false)
                .send(port.clone())
        })
        .collect();
    println!("Running {} pipelines", pipelines.len());
    if pipelines.len() > 1 {
        sched.add_task(merge(pipelines)).unwrap()
    } else {
        sched.add_task(pipelines.pop().unwrap()).unwrap()
    };
}

fn main() {
    let opts = basic_opts();

    let args: Vec<String> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };
    let configuration = read_matches(&matches, &opts);

    match initialize_system(&configuration) {
        Ok(mut context) => {
            context.start_schedulers();
            context.add_pipeline_to_run(Arc::new(move |p, s: &mut StandaloneScheduler| test(p, s)));
            context.execute();

            let mut pkts_so_far = (0, 0);
            let run_start = time::precise_time_s();
            let mut start = time::precise_time_ns() as f64 / CONVERSION_FACTOR;
            let sleep_time = Duration::from_millis(50);
            loop {
                let now = time::precise_time_ns() as f64 / CONVERSION_FACTOR;
                if now - start > 0.1 {
                    let mut rx = 0;
                    let mut tx = 0;
                    for port in context.ports.values() {
                        for q in 0..port.rxqs() {
                            let (rp, tp) = port.stats(q);
                            rx += rp;
                            tx += tp;
                        }
                    }
                    let pkts = (rx, tx);
                    println!(
                        "{} | {:.2} | {:.2} | {:.2}",
                        time::precise_time_s() - run_start,
                        now - start,
                        (pkts.0 - pkts_so_far.0) as f64 / (now - start),
                        (pkts.1 - pkts_so_far.1) as f64 / (now - start),
                        //state.rx_internal_flows.len(),
                        //state.rx_external_flows.len(),
                    );
                    start = now;
                    pkts_so_far = pkts;
                }
                thread::sleep(sleep_time); // Sleep for a bit
            }
        }
        Err(ref e) => {
            println!("Error: {}", e);
            if let Some(backtrace) = e.backtrace() {
                println!("Backtrace: {:?}", backtrace);
            }
            process::exit(1);
        }
    }
}
