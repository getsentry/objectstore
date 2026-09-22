use std::time::{Duration, SystemTime};

use objectstore_types::metadata::{self, ExpiryAnchor, MetadataUpdate};
use objectstore_types::time::Timestamp;

use crate::response::ResponseExt as _;
use crate::{ObjectKey, Session};

/// A requested minimum expiration deadline, without changing the expiration policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryExtension {
    /// An absolute deadline, rounded upward to a whole second.
    At(SystemTime),
    /// Total lifetime since the object's creation or most recent replacement.
    ///
    /// Fractional seconds are truncated when sent to the server.
    FromCreation(Duration),
    /// Lifetime from the start of this request on the server.
    ///
    /// Fractional seconds are truncated. Retrying establishes a new server-time
    /// anchor and can extend the deadline further.
    FromNow(Duration),
}

impl ExpiryExtension {
    fn into_update(self) -> crate::Result<MetadataUpdate> {
        let extend_expiry = match self {
            Self::At(at) => metadata::ExpiryExtension::At {
                at: Timestamp::try_from(at)
                    .map_err(metadata::Error::ExpirationTime)?
                    .as_rfc3339(),
            },
            Self::FromCreation(after) => metadata::ExpiryExtension::After {
                after,
                from: ExpiryAnchor::Creation,
            },
            Self::FromNow(after) => metadata::ExpiryExtension::After {
                after,
                from: ExpiryAnchor::Now,
            },
        };
        Ok(MetadataUpdate { extend_expiry })
    }
}

/// The result of a successful [`Session::extend_expiry`] call.
pub type ExtendExpiryResponse = ();

impl Session {
    /// Extends an object's expiration deadline without changing its policy or payload.
    ///
    /// An already-sufficient deadline succeeds without being shortened. Relative
    /// targets are resolved by the server, not added to the existing deadline.
    /// This requires object-write permission and a server supporting expiry updates.
    ///
    /// HTTP errors are returned through [`crate::Error::Reqwest`]. An object observed
    /// absent or expired returns 404. An extension rejected because the object is
    /// non-expiring, changed concurrently, or lacks creation metadata for a
    /// creation-relative target returns 409.
    /// Success does not report whether the deadline changed or its stored value.
    ///
    /// ```no_run
    /// # async fn example(session: objectstore_client::Session) -> objectstore_client::Result<()> {
    /// use std::time::Duration;
    /// use objectstore_client::ExpiryExtension;
    /// session.extend_expiry("key", ExpiryExtension::FromNow(Duration::from_secs(86400)))
    ///     .send().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn extend_expiry(&self, key: &str, target: ExpiryExtension) -> ExtendExpiryBuilder {
        ExtendExpiryBuilder {
            session: self.clone(),
            key: key.to_owned(),
            target,
        }
    }
}

/// A [`Session::extend_expiry`] request builder.
#[derive(Debug)]
pub struct ExtendExpiryBuilder {
    session: Session,
    key: ObjectKey,
    target: ExpiryExtension,
}

impl ExtendExpiryBuilder {
    /// Sends the extension request, failing locally for an out-of-range timestamp.
    pub async fn send(self) -> crate::Result<ExtendExpiryResponse> {
        let update = self.target.into_update()?;
        self.session
            .request(reqwest::Method::PATCH, &self.key)?
            .json(&update)
            .send()
            .await?
            .error_for_status_and_drain()
            .await?
            .drain_body()
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_targets() {
        for (target, expected) in [
            (
                ExpiryExtension::At(SystemTime::UNIX_EPOCH + Duration::from_millis(1500)),
                metadata::ExpiryExtension::At {
                    at: "1970-01-01T00:00:02Z".parse().unwrap(),
                },
            ),
            (
                ExpiryExtension::FromCreation(Duration::from_millis(1500)),
                metadata::ExpiryExtension::After {
                    after: Duration::from_millis(1500),
                    from: ExpiryAnchor::Creation,
                },
            ),
            (
                ExpiryExtension::FromNow(Duration::ZERO),
                metadata::ExpiryExtension::After {
                    after: Duration::ZERO,
                    from: ExpiryAnchor::Now,
                },
            ),
        ] {
            assert_eq!(target.into_update().unwrap().extend_expiry, expected);
        }
    }

    #[test]
    fn rejects_out_of_range_timestamps() {
        for at in [
            SystemTime::UNIX_EPOCH - Duration::from_secs(1),
            SystemTime::UNIX_EPOCH + Duration::from_secs(253_402_300_800),
        ] {
            assert!(matches!(
                ExpiryExtension::At(at).into_update(),
                Err(crate::Error::Metadata(metadata::Error::ExpirationTime(_)))
            ));
        }
    }
}
