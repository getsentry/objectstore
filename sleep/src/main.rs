use std::env;
use std::thread;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <seconds>", args[0]);
        std::process::exit(1);
    }

    let seconds: u64 = args[1].parse().unwrap_or_else(|_| {
        eprintln!("Error: '{}' is not a valid number of seconds", args[1]);
        std::process::exit(1);
    });

    thread::sleep(Duration::from_secs(seconds));
}
