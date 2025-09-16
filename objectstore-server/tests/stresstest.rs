use std::process::{Child, Command, Stdio};
use std::time::Duration;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use stresstest::Workload;
use stresstest::http::HttpRemote;

const OBJECTSTORE_EXE: &str = env!("CARGO_BIN_EXE_objectstore");

fn assert_clean_shutdown(mut child: Child) {
    let pid = Pid::from_raw(child.id() as i32);
    signal::kill(pid, Signal::SIGINT).expect("Failed to send SIGINT");

    let output = child.wait().expect("Failed to wait on child process");

    assert!(
        output.success(),
        "Process exited with non-zero status: {:?}",
        output.code()
    );
}

#[tokio::test]
async fn test_basic() {
    let tempdir = tempfile::tempdir().unwrap();
    let port = 10000 + rand::random::<u16>() % 10000;
    let addr = format!("127.0.0.1:{port}");

    let mut cmd = Command::new(OBJECTSTORE_EXE);
    cmd.env("FSS_HTTP_ADDR", &addr)
        .env("FSS_high_volume_storage__TYPE", "filesystem")
        .env(
            "FSS_high_volume_storage__PATH",
            tempdir.path().display().to_string(),
        )
        .env("FSS_long_term_storage__TYPE", "filesystem")
        .env(
            "FSS_long_term_storage__PATH",
            tempdir.path().display().to_string(),
        )
        .env("RUST_LOG", "warn");
    if let Ok(datadog_key) = std::env::var("DD_API_KEY") {
        cmd.env("FSS_datadog_key", datadog_key);
    }
    let child = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to spawn subprocess");

    // Give the server time to start, or else stresstest might fail to connect.
    std::thread::sleep(Duration::from_secs(1));

    let remote = HttpRemote::new(&format!("http://{addr}"));
    let workload = Workload::builder("test")
        .concurrency(10)
        .size_distribution(1000, 10_000)
        .action_weights(8, 1, 1)
        .build();

    stresstest::run(remote, vec![workload], Duration::from_secs(2))
        .await
        .expect("Failed to run stress test");

    assert_clean_shutdown(child);
}
