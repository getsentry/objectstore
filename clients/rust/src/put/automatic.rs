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
    let mut query_progress = false;
    loop {
        let response = if query_progress {
            handle.progress().send().await
        } else {
            handle
                .put_stream(offset, body.len() - offset, body.stream(offset).await?)
                .send()
                .await
        };
        let failure = match upload_offset(response) {
            Ok(None) => return Ok(()),
            Ok(Some(next)) => {
                if next < offset || next > body.len() {
                    return Err(Error::MalformedResponse("invalid upload offset".into()));
                }
                if next > offset || query_progress {
                    if next > offset {
                        retries = 0;
                    }
                    offset = next;
                    query_progress = false;
                    continue;
                }
                io::Error::other("upload made no progress").into()
            }
            Err(error) if is_transient(&error) => error,
            Err(error) => return Err(error),
        };

        // Writes and progress queries share a budget, reset only by an advancing offset.
        if retries >= 2 {
            return Err(failure);
        }
        let delay =
            Duration::from_secs(2 << retries) + Duration::from_secs_f64(rand::random::<f64>());
        tokio::time::sleep(delay).await;
        retries += 1;
        query_progress = true;
    }
}

fn upload_offset(result: crate::Result<UploadProgress>) -> crate::Result<Option<u64>> {
    match result {
        Ok(UploadProgress::Complete) => Ok(None),
        Ok(UploadProgress::Incomplete { offset }) | Err(Error::UploadOffsetMismatch { offset }) => {
            Ok(Some(offset))
        }
        Err(error) => Err(error),
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
