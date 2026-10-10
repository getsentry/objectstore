//! These tests use the Cassandra devservice. CQL_TEST_CONFIG may name a JSON CqlConfig
//! file to run the same suite against another pre-provisioned Cassandra or Scylla instance.

use super::*;
use crate::backend::common::ExpiryTarget;
use crate::id::ObjectContext;
use crate::stream;
use objectstore_types::metadata::ExpirationPolicy;
use objectstore_types::scope::{Scope, Scopes};

fn test_config() -> CqlConfig {
    if let Some(path) = std::env::var_os("CQL_TEST_CONFIG") {
        return serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    }
    CqlConfig {
        nodes: vec!["localhost:9042".into()],
        keyspace: "objectstore".into(),
        table_name: "objects".into(),
        local_datacenter: "datacenter1".into(),
        username: None,
        password: None,
        tls_ca_bundle: None,
        request_timeout: default_request_timeout(),
        cogs: None,
    }
}

async fn backend() -> anyhow::Result<CqlBackend> {
    CqlBackend::new(test_config(), &ChangeStreamFactory::default()).await
}

fn id() -> ObjectId {
    ObjectId::random(ObjectContext {
        usecase: "testing".into(),
        scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
    })
}

fn expiring(now: Timestamp, seconds: u64) -> Metadata {
    Metadata {
        time_created: Some(now),
        time_expires: Some(now + Duration::from_secs(seconds)),
        expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(seconds)),
        ..Metadata::default()
    }
}

async fn put(
    backend: &CqlBackend,
    id: &ObjectId,
    metadata: &Metadata,
    bytes: &'static str,
) -> Result<()> {
    backend
        .put_object(id, metadata, stream::single(bytes), Timestamp::now())
        .await
}

async fn body(backend: &CqlBackend, id: &ObjectId) -> Result<Vec<u8>> {
    let (_, _, stream) = backend
        .get_object(id, Timestamp::now(), None)
        .await?
        .unwrap();
    stream::read_to_vec(stream).await
}

#[test]
fn configuration_validation_and_redaction() {
    let mut config = test_config();
    config.validate().unwrap();
    config.username = Some("private-user".into());
    assert!(config.validate().is_err());
    config.password = Some("private-password".into());
    config.validate().unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("private-user"));
    assert!(!debug.contains("private-password"));
    config.table_name = "objects; DROP TABLE objects".into();
    assert!(config.validate().is_err());
    for name in ["", "UPPER", "1table", "with space", "quoted\"name"] {
        assert!(!valid_identifier(name));
    }
    assert!(valid_identifier("objects_2"));
    config.table_name = "objects".into();
    config.nodes.clear();
    assert!(config.validate().is_err());
    config.nodes.push("localhost".into());
    config.local_datacenter.clear();
    assert!(config.validate().is_err());
    config.local_datacenter = "datacenter1".into();
    config.request_timeout = Duration::ZERO;
    assert!(config.validate().is_err());
}

#[test]
fn ttl_boundaries() {
    let now = 1_800_000_000;
    let timestamp = |seconds| Some(Timestamp::from_unix_secs(seconds).unwrap());
    assert_eq!(native_ttl(None, now).unwrap(), 0);
    assert_eq!(native_ttl(timestamp(now - 1), now).unwrap(), 1);
    assert_eq!(native_ttl(timestamp(now), now).unwrap(), 1);
    assert_eq!(native_ttl(timestamp(now + 10), now).unwrap(), 11);
    assert_eq!(
        native_ttl(timestamp(MAX_EXPIRATION - 1), now).unwrap() as u64,
        MAX_EXPIRATION - now
    );
    assert_eq!(
        native_ttl(timestamp(MAX_EXPIRATION), now)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidMetadata
    );
    assert_eq!(
        native_ttl(timestamp(MAX_TTL - 1), 0).unwrap() as u64,
        MAX_TTL
    );
    assert_eq!(
        native_ttl(timestamp(MAX_TTL), 0).unwrap_err().kind(),
        ErrorKind::InvalidMetadata
    );
}

#[test]
fn lwt_applied_decoding() {
    for columns in [
        vec![Some(CqlValue::Boolean(true))],
        vec![Some(CqlValue::Boolean(true)), None, Some(CqlValue::Int(2))],
    ] {
        assert!(applied_value(columns.first()).unwrap());
    }
    assert!(!applied_value(Some(&Some(CqlValue::Boolean(false)))).unwrap());
    for column in [None, Some(None), Some(Some(CqlValue::Int(1)))] {
        assert_eq!(
            applied_value(column.as_ref()).unwrap_err().kind(),
            ErrorKind::CorruptData
        );
    }
}

#[tokio::test]
async fn rejects_invalid_tls_bundle() -> anyhow::Result<()> {
    let pem = tempfile::NamedTempFile::new()?;
    let mut config = test_config();
    config.tls_ca_bundle = Some(pem.path().into());
    assert!(
        CqlBackend::new(config, &ChangeStreamFactory::default())
            .await
            .unwrap_err()
            .to_string()
            .contains("no certificates")
    );
    Ok(())
}

#[tokio::test]
async fn object_roundtrip_ranges_and_delete() -> anyhow::Result<()> {
    let backend = backend().await?;
    let id = id();
    let now = Timestamp::now();
    assert!(backend.get_object(&id, now, None).await?.is_none());
    assert!(backend.get_metadata(&id, now).await?.is_none());
    backend.delete_object(&id, now).await?;
    let mut metadata = expiring(now, 3600);
    metadata.content_type = "text/plain".into();
    metadata.custom.insert("key".into(), "value".into());
    put(&backend, &id, &metadata, "hello world").await?;
    let actual = backend.get_metadata(&id, now).await?.unwrap();
    assert_eq!(actual.time_expires, metadata.time_expires);
    assert_eq!(actual.time_created, metadata.time_created);
    assert_eq!(actual.custom, metadata.custom);
    assert_eq!(actual.content_type, metadata.content_type);
    assert_eq!(actual.size, Some(11));
    assert_eq!(body(&backend, &id).await?, b"hello world");
    for (range, expected, start) in [
        (ByteRange::Bounded(1, 3), "ell", 1),
        (ByteRange::From(6), "world", 6),
        (ByteRange::Last(3), "rld", 8),
    ] {
        let (_, range, stream) = backend.get_object(&id, now, Some(range)).await?.unwrap();
        let range = range.unwrap();
        assert_eq!(range.start, start);
        assert_eq!(range.total, 11);
        assert_eq!(stream::read_to_vec(stream).await?, expected.as_bytes());
    }
    assert_eq!(
        backend
            .get_object(&id, now, Some(ByteRange::From(99)))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::RangeNotSatisfiable { total: 11 }
    );
    put(&backend, &id, &Metadata::default(), "").await?;
    assert!(body(&backend, &id).await?.is_empty());
    assert_eq!(backend.get_metadata(&id, now).await?.unwrap().size, Some(0));
    backend.delete_object(&id, now).await?;
    assert!(backend.get_metadata(&id, now).await?.is_none());
    backend.delete_object(&id, now).await?;
    Ok(())
}

#[tokio::test]
async fn expiry_outcomes_and_policy() -> anyhow::Result<()> {
    let backend = backend().await?;
    let id = id();
    let now = Timestamp::now();
    let metadata = expiring(now, 60);
    let deadline = metadata.time_expires.unwrap();
    let update = ExpiryTarget::At(now + Duration::from_secs(120));
    assert_eq!(
        backend.set_expiry(&id, update.into(), now).await?,
        SetExpiryResponse::NotFound
    );
    put(&backend, &id, &Metadata::default(), "manual").await?;
    assert_eq!(
        backend.set_expiry(&id, update.into(), now).await?,
        SetExpiryResponse::Rejected
    );
    put(&backend, &id, &metadata, "payload").await?;
    assert!(backend.get_metadata(&id, deadline).await?.is_some());
    assert!(
        backend
            .get_metadata(&id, deadline + Duration::from_secs(1))
            .await?
            .is_none()
    );
    assert_eq!(
        backend
            .set_expiry(&id, update.into(), deadline + Duration::from_secs(1))
            .await?,
        SetExpiryResponse::NotFound
    );
    assert_eq!(
        backend
            .set_expiry(&id, ExpiryTarget::At(deadline).into(), now)
            .await?,
        SetExpiryResponse::Satisfied(deadline)
    );
    assert_eq!(
        backend
            .set_expiry(
                &id,
                ExpiryUpdate {
                    target: ExpiryTarget::At(deadline),
                    max: Some(Duration::from_secs(1))
                },
                now
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidMetadata
    );
    let extended = now + Duration::from_secs(120);
    assert_eq!(
        backend
            .set_expiry(
                &id,
                ExpiryTarget::FromCreation(Duration::from_secs(120)).into(),
                now
            )
            .await?,
        SetExpiryResponse::Satisfied(extended)
    );
    let actual = backend.get_metadata(&id, now).await?.unwrap();
    assert_eq!(actual.time_expires, Some(extended));
    assert_eq!(
        actual.expiration_policy,
        ExpirationPolicy::TimeToLive(Duration::from_secs(120))
    );
    assert_eq!(body(&backend, &id).await?, b"payload");
    let tti = Metadata {
        expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(60)),
        ..metadata
    };
    put(&backend, &id, &tti, "tti").await?;
    backend.set_expiry(&id, update.into(), now).await?;
    assert_eq!(
        backend
            .get_metadata(&id, now)
            .await?
            .unwrap()
            .expiration_policy,
        tti.expiration_policy
    );
    let far_future = Timestamp::from_unix_secs(MAX_EXPIRATION).unwrap();
    assert_eq!(
        backend
            .set_expiry(&id, ExpiryTarget::At(far_future).into(), now)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidMetadata
    );
    let no_creation = Metadata {
        time_created: None,
        ..tti
    };
    put(&backend, &id, &no_creation, "no creation").await?;
    assert_eq!(
        backend
            .set_expiry(
                &id,
                ExpiryTarget::FromCreation(Duration::from_secs(120)).into(),
                now
            )
            .await?,
        SetExpiryResponse::Rejected
    );
    backend.delete_object(&id, now).await?;
    Ok(())
}

/// Poll native removal without applying the backend's logical expiry filter.
async fn wait_for_expiration(backend: &CqlBackend, id: &ObjectId) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if backend.read_record(OBJECTS, id, true).await?.is_none() {
                return Ok::<_, Error>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn native_ttl_renewal_and_nonexpiring_transitions() -> anyhow::Result<()> {
    let backend = backend().await?;
    let now = Timestamp::now();
    let expires = id();
    let renewed = id();
    let manual = id();
    let short = expiring(now, 2);
    put(&backend, &expires, &Metadata::default(), "formerly manual").await?;
    put(&backend, &expires, &short, "expires").await?;
    put(&backend, &renewed, &short, "renewed").await?;
    put(&backend, &manual, &short, "temporary").await?;
    put(&backend, &manual, &Metadata::default(), "permanent").await?;
    backend
        .set_expiry(
            &renewed,
            ExpiryTarget::At(now + Duration::from_secs(60)).into(),
            now,
        )
        .await?;
    wait_for_expiration(&backend, &expires).await?;
    assert_eq!(body(&backend, &renewed).await?, b"renewed");
    assert_eq!(body(&backend, &manual).await?, b"permanent");
    // A new incarnation can be created after every cell from the old one expires.
    put(&backend, &expires, &Metadata::default(), "recreated").await?;
    assert_eq!(body(&backend, &expires).await?, b"recreated");
    for id in [&expires, &renewed, &manual] {
        backend.delete_object(id, now).await?;
    }
    Ok(())
}

#[tokio::test]
async fn redirects_and_conditional_transitions() -> anyhow::Result<()> {
    let backend = backend().await?;
    let key = id();
    let now = Timestamp::now();
    let first = Tombstone {
        target: id(),
        time_expires: Some(now + Duration::from_secs(60)),
    };
    let second = Tombstone {
        target: id(),
        time_expires: None,
    };
    let write = || TieredWrite::Tombstone(first.clone());
    assert!(backend.compare_and_write(&key, None, write(), now).await?);
    assert!(backend.compare_and_write(&key, None, write(), now).await?);
    assert_eq!(
        backend
            .put_non_tombstone(
                &key,
                &Metadata::default(),
                Bytes::from_static(b"blocked"),
                now
            )
            .await?,
        Some(first.clone())
    );
    assert_eq!(
        backend.delete_non_tombstone(&key, now).await?,
        Some(first.clone())
    );
    assert!(
        matches!(backend.get_tiered_object(&key, now, None).await?, TieredGet::Tombstone(t) if t == first)
    );
    assert!(
        matches!(backend.get_tiered_metadata(&key, now).await?, TieredMetadata::Tombstone(t) if t == first)
    );
    assert_eq!(
        backend.get_metadata(&key, now).await.unwrap_err().kind(),
        ErrorKind::UnexpectedTombstone
    );
    assert!(
        !backend
            .compare_and_write(&key, Some(&second.target), TieredWrite::Delete, now)
            .await?
    );
    let extension = ExpiryTarget::At(now + Duration::from_secs(120));
    assert_eq!(
        backend
            .compare_and_update(&key, None, TieredUpdate::SetExpiry(extension.into()), now)
            .await?,
        SetExpiryResponse::Rejected
    );
    assert_eq!(
        backend
            .compare_and_update(
                &key,
                Some(&second.target),
                TieredUpdate::SetExpiry(extension.into()),
                now
            )
            .await?,
        SetExpiryResponse::Rejected
    );
    assert_eq!(
        backend
            .compare_and_update(
                &key,
                Some(&first.target),
                TieredUpdate::SetExpiry(
                    ExpiryTarget::FromCreation(Duration::from_secs(120)).into()
                ),
                now
            )
            .await?,
        SetExpiryResponse::Rejected
    );
    assert_eq!(
        backend
            .compare_and_update(
                &key,
                Some(&first.target),
                TieredUpdate::SetExpiry(extension.into()),
                now
            )
            .await?,
        SetExpiryResponse::Satisfied(now + Duration::from_secs(120))
    );
    assert!(
        backend
            .compare_and_write(
                &key,
                Some(&first.target),
                TieredWrite::Tombstone(second.clone()),
                now
            )
            .await?
    );
    // Repeating the swap recognizes its destination even though the old target no longer matches.
    assert!(
        backend
            .compare_and_write(
                &key,
                Some(&first.target),
                TieredWrite::Tombstone(second.clone()),
                now
            )
            .await?
    );
    assert!(
        backend
            .compare_and_write(
                &key,
                Some(&second.target),
                TieredWrite::Object(Metadata::default(), Bytes::from_static(b"inline")),
                now
            )
            .await?
    );
    assert_eq!(body(&backend, &key).await?, b"inline");
    assert!(
        backend
            .compare_and_write(&key, None, TieredWrite::Delete, now)
            .await?
    );
    assert!(
        backend
            .compare_and_write(&key, None, TieredWrite::Delete, now)
            .await?
    );
    assert!(
        backend
            .compare_and_write(
                &key,
                None,
                TieredWrite::Object(Metadata::default(), Bytes::from_static(b"new")),
                now
            )
            .await?
    );
    // Ordinary Backend writes and deletes also replace redirects atomically.
    assert!(backend.compare_and_write(&key, None, write(), now).await?);
    put(&backend, &key, &Metadata::default(), "ordinary").await?;
    assert_eq!(body(&backend, &key).await?, b"ordinary");
    backend.compare_and_write(&key, None, write(), now).await?;
    backend.delete_object(&key, now).await?;
    Ok(())
}

#[tokio::test]
async fn expired_redirects_do_not_block_mutations() -> anyhow::Result<()> {
    let backend = backend().await?;
    let now = Timestamp::now();
    let expired_at = now + Duration::from_secs(60);
    let access = expired_at + Duration::from_secs(1);
    for operation in 0..3 {
        let key = id();
        backend
            .compare_and_write(
                &key,
                None,
                TieredWrite::Tombstone(Tombstone {
                    target: id(),
                    time_expires: Some(expired_at),
                }),
                now,
            )
            .await?;
        assert!(matches!(
            backend.get_tiered_object(&key, access, None).await?,
            TieredGet::NotFound
        ));
        match operation {
            0 => assert!(
                backend
                    .put_non_tombstone(
                        &key,
                        &Metadata::default(),
                        Bytes::from_static(b"new"),
                        access
                    )
                    .await?
                    .is_none()
            ),
            1 => assert!(backend.delete_non_tombstone(&key, access).await?.is_none()),
            _ => assert!(
                backend
                    .compare_and_write(
                        &key,
                        None,
                        TieredWrite::Object(Metadata::default(), Bytes::from_static(b"new")),
                        access
                    )
                    .await?
            ),
        }
        backend.delete_object(&key, now).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_revision_cannot_overwrite_delete_or_renew_new_data() -> anyhow::Result<()> {
    let backend = backend().await?;
    let key = id();
    let now = Timestamp::now();
    put(&backend, &key, &expiring(now, 60), "old").await?;
    let mut stale = backend.read_record(OBJECTS, &key, true).await?.unwrap();
    put(&backend, &key, &expiring(now, 60), "new").await?;
    if let Record::Object(metadata, _) = &mut stale.record {
        metadata.time_expires = Some(now + Duration::from_secs(120));
    }
    assert!(
        !backend
            .write_record(OBJECTS, &key, Some(stale.revision), &stale.record, true)
            .await?
    );
    assert!(!backend.delete_record(OBJECTS, &key, stale.revision).await?);
    assert_eq!(body(&backend, &key).await?, b"new");
    backend.delete_object(&key, now).await?;
    assert!(
        !backend
            .write_record(OBJECTS, &key, Some(stale.revision), &stale.record, true)
            .await?
    );
    assert!(backend.get_metadata(&key, now).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn competing_redirects_have_one_winner() -> anyhow::Result<()> {
    let backend = backend().await?;
    let key = id();
    let now = Timestamp::now();
    let first = TieredWrite::Tombstone(Tombstone {
        target: id(),
        time_expires: None,
    });
    let second = TieredWrite::Tombstone(Tombstone {
        target: id(),
        time_expires: None,
    });
    let (a, b) = tokio::join!(
        backend.compare_and_write(&key, None, first, now),
        backend.compare_and_write(&key, None, second, now)
    );
    assert_ne!(a?, b?);
    backend.delete_object(&key, now).await?;
    Ok(())
}

#[tokio::test]
async fn upload_marker_isolation_expiry_and_single_consumption() -> anyhow::Result<()> {
    let backend = backend().await?;
    let key = id();
    let now = Timestamp::now();
    let deadline = now + Duration::from_secs(60);
    assert!(!backend.has_upload_marker(&key, now).await?);
    assert!(!backend.delete_upload_marker(&key, now).await?);
    backend.create_upload_marker(&key, deadline).await?;
    assert!(backend.get_metadata(&key, now).await?.is_none());
    assert!(backend.has_upload_marker(&key, deadline).await?);
    assert!(
        !backend
            .has_upload_marker(&key, deadline + Duration::from_secs(1))
            .await?
    );
    assert!(
        !backend
            .delete_upload_marker(&key, deadline + Duration::from_secs(1))
            .await?
    );
    put(&backend, &key, &Metadata::default(), "object").await?;
    let (a, b) = tokio::join!(
        backend.delete_upload_marker(&key, now),
        backend.delete_upload_marker(&key, now)
    );
    assert_ne!(a?, b?);
    assert!(!backend.delete_upload_marker(&key, now).await?);
    assert_eq!(body(&backend, &key).await?, b"object");
    backend.delete_object(&key, now).await?;
    Ok(())
}

#[cfg(feature = "storage-cogs")]
#[tokio::test]
async fn change_stream_only_reports_applied_object_mutations() -> anyhow::Result<()> {
    use objectstore_inventory_tracker::OpType;
    let (streams, producer) = crate::change_stream::dummy_factory();
    let mut config = test_config();
    config.cogs = Some(CostTrackerStreamConfig {
        shared_resource_id: "cql_objectstore".into(),
        sample_rate: 1.0,
    });
    let backend = CqlBackend::new(config, &streams).await?;
    let key = id();
    let now = Timestamp::now();
    put(&backend, &key, &expiring(now, 60), "payload").await?;
    let stale = backend.read_record(OBJECTS, &key, true).await?.unwrap();
    backend
        .set_expiry(
            &key,
            ExpiryTarget::At(now + Duration::from_secs(120)).into(),
            now,
        )
        .await?;
    assert!(
        !backend
            .write_record(OBJECTS, &key, Some(stale.revision), &stale.record, true)
            .await?
    );
    // An already-satisfied update and upload markers emit no events.
    backend
        .set_expiry(
            &key,
            ExpiryTarget::At(now + Duration::from_secs(60)).into(),
            now,
        )
        .await?;
    backend
        .create_upload_marker(&key, now + Duration::from_secs(60))
        .await?;
    backend.delete_upload_marker(&key, now).await?;
    backend.delete_object(&key, now).await?;
    backend.join().await;
    let records = producer.records();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].op_type, OpType::Write);
    assert_eq!(records[0].shared_resource_id, "cql_objectstore");
    assert_eq!(records[0].app_feature, "testing");
    assert!(records[0].size.unwrap() > 7);
    assert_eq!(records[1].op_type, OpType::Update);
    assert_eq!(records[2].op_type, OpType::Delete);
    assert_eq!(records[0].record_id, records[2].record_id);
    Ok(())
}
