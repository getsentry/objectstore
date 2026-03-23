use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

fn list_crates() -> Vec<String> {
    let mut crates = Vec::new();

    for result in fs::read_dir("../").unwrap() {
        let entry = result.unwrap();

        if !entry.file_type().unwrap().is_dir() {
            continue;
        }

        // Only include directories that are actual Cargo packages.
        if !entry.path().join("Cargo.toml").exists() {
            continue;
        }

        if let Some(s) = entry.file_name().to_str()
            && s.starts_with("objectstore")
        {
            // Cargo crate names use underscores; directory names use hyphens.
            crates.push(s.replace('-', "_"));
        }
    }

    crates.sort();
    crates
}

fn main() {
    println!("cargo:rerun-if-changed=../");
    let crates = list_crates();

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("constants.gen.rs");
    let mut f = File::create(dest_path).unwrap();

    write!(f, "const CRATE_NAMES: &[&str] = &[").unwrap();
    for name in &crates {
        write!(f, "\"{name}\",").unwrap();
    }
    writeln!(f, "];").unwrap();
}
