use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use argh::FromArgs;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use objectstore_client::{
    Client, ExpirationPolicy, OperationResult, SecretKey, TokenGenerator, Usecase,
};

/// Bulk file upload tool for Objectstore
#[derive(Debug, FromArgs)]
struct Args {
    /// path to the directory containing files to upload
    #[argh(option, short = 'p')]
    path: PathBuf,

    /// URL of the Objectstore service
    #[argh(option, short = 'u')]
    url: String,

    /// maximum number of files to upload
    #[argh(option, short = 'l')]
    limit: Option<usize>,

    /// path to the EdDSA private key for authentication
    #[argh(option)]
    key_path: Option<PathBuf>,

    /// key ID for authentication
    #[argh(option)]
    kid: Option<String>,

    /// pre-signed JWT auth token (alternative to --key-path/--kid)
    #[argh(option)]
    token: Option<String>,

    /// max concurrent individual (non-batch) requests
    #[argh(option)]
    individual_concurrency: Option<usize>,

    /// max concurrent batch requests
    #[argh(option)]
    batch_concurrency: Option<usize>,

    /// send HEAD requests for all keys before uploading
    #[argh(switch, short = 'h')]
    head_first: bool,

    /// prefix for object keys
    #[argh(option)]
    prefix: Option<String>,
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn collect_files(dir: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_files_inner(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files_inner(dir: &PathBuf, files: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("failed to read directory {dir:?}"))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_inner(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = argh::from_env();

    let mut files = collect_files(&args.path)?;
    println!("Found {} files in {:?}", files.len(), args.path);

    if let Some(limit) = args.limit {
        files.truncate(limit);
        println!("Limited to {} files", files.len());
    }

    if files.is_empty() {
        println!("No files to upload");
        return Ok(());
    }

    let has_keypair = args.key_path.is_some() || args.kid.is_some();
    if has_keypair && args.token.is_some() {
        anyhow::bail!("--token and --key-path/--kid are mutually exclusive");
    }

    let mut builder = Client::builder(&args.url)
        .timeout(std::time::Duration::from_secs(30))
        .configure_reqwest(|b| b.connect_timeout(std::time::Duration::from_secs(30)));
    if let Some(token) = args.token {
        builder = builder.token(token);
    } else if let (Some(key_path), Some(kid)) = (args.key_path, args.kid) {
        let key = std::fs::read_to_string(&key_path)
            .with_context(|| format!("failed to read private key from {key_path:?}"))?;
        builder = builder.token(TokenGenerator::new(SecretKey {
            kid,
            secret_key: key,
        })?);
    } else if has_keypair {
        anyhow::bail!("--key-path and --kid must be provided together");
    }
    let client = builder.build().context("failed to build client")?;

    let usecase = Usecase::new("preprod").with_expiration_policy(ExpirationPolicy::TimeToIdle(
        std::time::Duration::from_secs(86400),
    ));
    let scope = usecase.for_project(4510127168028672, 4511035488272480);
    let session = scope.session(&client).context("failed to create session")?;

    let base = &args.path;
    let keys: Vec<String> = files
        .iter()
        .map(|file| {
            let relative = file
                .strip_prefix(base)
                .unwrap_or(file)
                .to_string_lossy()
                .to_string();
            match &args.prefix {
                Some(prefix) => format!("{prefix}/{relative}"),
                None => relative,
            }
        })
        .collect();

    if args.head_first {
        println!("Sending HEAD requests for {} keys...", keys.len());
        let mut head_many = session.many();
        if let Some(c) = args.individual_concurrency {
            head_many = head_many.max_individual_concurrency(c);
        }
        if let Some(c) = args.batch_concurrency {
            head_many = head_many.max_batch_concurrency(c);
        }
        for key in &keys {
            head_many = head_many.push(session.head(key));
        }

        let head_pb = ProgressBar::new(keys.len() as u64);
        head_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [HEAD] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
                .progress_chars("#>-"),
        );

        let head_start = Instant::now();
        let mut head_results = head_many.send().await;
        let mut head_found = 0u64;
        let mut head_missing = 0u64;
        let mut head_errors = 0u64;

        while let Some(result) = head_results.next().await {
            match result {
                OperationResult::Head(_key, Ok(Some(_))) => {
                    head_found += 1;
                    head_pb.inc(1);
                }
                OperationResult::Head(_key, Ok(None)) => {
                    head_missing += 1;
                    head_pb.inc(1);
                }
                OperationResult::Head(_key, Err(_)) => {
                    head_errors += 1;
                    head_pb.inc(1);
                }
                OperationResult::Error(_) => {
                    head_errors += 1;
                    head_pb.inc(1);
                }
                _ => {}
            }
        }

        head_pb.finish_and_clear();
        let head_elapsed = head_start.elapsed();
        let head_total = head_found + head_missing + head_errors;
        let head_per_key = if head_total > 0 {
            head_elapsed / head_total as u32
        } else {
            std::time::Duration::ZERO
        };

        println!();
        println!(
            "HEAD: {head_found} found, {head_missing} missing, {head_errors} errors (out of {head_total})"
        );
        println!("HEAD total duration: {head_elapsed:.2?}");
        println!("HEAD duration per key: {head_per_key:.2?}");
        println!();
    }

    let mut many = session.many();
    if let Some(c) = args.individual_concurrency {
        many = many.max_individual_concurrency(c);
    }
    if let Some(c) = args.batch_concurrency {
        many = many.max_batch_concurrency(c);
    }
    for (file, key) in files.iter().zip(&keys) {
        many = many.push(session.put_path(file).key(key));
    }

    let total = files.len() as u64;
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [PUT] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"),
    );

    let start = Instant::now();
    let mut results = many.send().await;

    let mut success = 0u64;
    let mut errors = 0u64;

    while let Some(result) = results.next().await {
        match result {
            OperationResult::Put(_key, Ok(_)) => {
                success += 1;
                pb.inc(1);
            }
            OperationResult::Put(_key, Err(_)) => {
                errors += 1;
                pb.inc(1);
            }
            OperationResult::Error(_) => {
                errors += 1;
                pb.inc(1);
            }
            _ => {}
        }
    }

    pb.finish_and_clear();

    let elapsed = start.elapsed();
    let total_files = success + errors;
    let per_file = if total_files > 0 {
        elapsed / total_files as u32
    } else {
        std::time::Duration::ZERO
    };

    println!();
    println!("Uploaded {success}/{total_files} files ({errors} errors)");
    println!("Total duration: {elapsed:.2?}");
    println!("Duration per file: {per_file:.2?}");

    Ok(())
}
