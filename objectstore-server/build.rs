use std::io;
use std::process::{Command, Stdio};

const REVISION_ENV: &str = "OBJECTSTORE_BUILD_REVISION";

fn git_revision() -> Result<String, io::Error> {
    let cmd = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .stderr(Stdio::inherit())
        .output()?;

    if !cmd.status.success() {
        return Err(io::Error::other(format!(
            "`git rev-parse' failed: {}",
            cmd.status
        )));
    }

    Ok(String::from_utf8_lossy(&cmd.stdout).trim().to_owned())
}

fn emit_release_var() -> Result<(), io::Error> {
    println!("cargo:rerun-if-env-changed={REVISION_ENV}");
    let revision = match std::env::var(REVISION_ENV) {
        Ok(revision) => revision,
        Err(std::env::VarError::NotPresent) => git_revision()?,
        Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
    };
    let version = std::env::var("CARGO_PKG_VERSION").unwrap();
    println!("cargo:rustc-env=OBJECTSTORE_RELEASE=objectstore@{version}+{revision}");

    Ok(())
}

fn main() {
    emit_release_var().unwrap();
}
