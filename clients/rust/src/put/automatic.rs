//! Write prepared payloads, recovering from authoritative server progress.

use std::io;
use std::time::Duration;

use super::PutResponse;
use super::payload::ReplayBody;
use crate::Error;
use crate::resumable::{ResumableUpload, UploadProgress};

pub(super) async fn upload(
    mut body: ReplayBody,
    handle: ResumableUpload,
) -> crate::Result<PutResponse> {
    match upload_inner(&mut body, &handle).await {
        Ok(()) => Ok(PutResponse {
            key: handle.key().to_owned(),
        }),
        Err(error) => {
            let _ = handle.cancel().send().await;
            Err(Error::UploadFailed(Box::new(error)))
        }
    }
}

async fn upload_inner(body: &mut ReplayBody, handle: &ResumableUpload) -> crate::Result<()> {
    let mut offset = 0;
    let mut retries = 0;
    loop {
        let remaining = body.len() - offset;
        let write = body
            .with_stream(offset, |stream| async {
                handle.put_stream(offset, remaining, stream).send().await
            })
            .await;
        let progress = match authoritative_progress(write) {
            Ok(UploadProgress::Incomplete { offset: next }) if next == offset => {
                let error = io::Error::other("upload made no progress").into();
                recover(handle, &mut retries, error).await?
            }
            Err(error) if is_transient(&error) => recover(handle, &mut retries, error).await?,
            other => other?,
        };
        match progress {
            UploadProgress::Complete => return Ok(()),
            UploadProgress::Incomplete { offset: next } => {
                if next < offset || next > body.len() {
                    return Err(Error::MalformedResponse("invalid upload offset".into()));
                }
                if next > offset {
                    retries = 0;
                }
                offset = next;
            }
        }
    }
}

/// Writes and failed probes share this budget; only acknowledged progress resets it.
async fn recover(
    handle: &ResumableUpload,
    retries: &mut u32,
    mut failure: Error,
) -> crate::Result<UploadProgress> {
    while *retries < 2 {
        let delay =
            Duration::from_secs(2 << *retries) + Duration::from_secs_f64(rand::random::<f64>());
        tokio::time::sleep(delay).await;
        *retries += 1;
        match authoritative_progress(handle.progress().send().await) {
            Err(error) if is_transient(&error) => failure = error,
            result => return result,
        }
    }
    Err(failure)
}

fn authoritative_progress(result: crate::Result<UploadProgress>) -> crate::Result<UploadProgress> {
    match result {
        Err(Error::UploadOffsetMismatch { offset }) => Ok(UploadProgress::Incomplete { offset }),
        other => other,
    }
}

fn is_transient(error: &Error) -> bool {
    match error {
        Error::Reqwest(error) => match error.status() {
            Some(status) => matches!(status.as_u16(), 408 | 429 | 502 | 503 | 504),
            None => {
                error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
            }
        },
        _ => false,
    }
}
