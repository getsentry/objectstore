//! Contains the implementation for the automatic resumable upload loop.

use std::io;
use std::time::Duration;

use super::PutResponse;
use super::payload::{ReadError, ReplayableBody};
use crate::Error;
use crate::resumable::implementation::{ResumableUpload, UploadProgress};

pub(super) async fn upload(
    body: &ReplayableBody,
    handle: ResumableUpload,
) -> crate::Result<PutResponse> {
    match upload_inner(body, &handle).await {
        Ok(()) => Ok(PutResponse {
            key: handle.key().to_owned(),
        }),
        Err(error) => {
            let _ = handle.cancel().send().await;
            Err(Error::UploadFailed(Box::new(error)))
        }
    }
}

async fn upload_inner(body: &ReplayableBody, handle: &ResumableUpload) -> crate::Result<()> {
    let mut offset = 0;
    let mut retries = 0;
    loop {
        let remaining = body.len() - offset;
        let write = handle
            .put_stream(offset, remaining, body.stream(offset).await?)
            .send()
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
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = source {
        if error
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
            .is_some_and(|inner| inner.is::<ReadError>())
        {
            return false;
        }
        source = error.source();
    }
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
