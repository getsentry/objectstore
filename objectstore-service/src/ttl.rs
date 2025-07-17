use std::collections::{BTreeMap, BTreeSet};

type ObjectId = usize;
type Timestamp = usize;
type Duration = usize;

const TTI_REFRESH_DEBOUNCE: Duration = 2; // TODO

#[derive(Debug)]
enum ExpirationPolicy {
    TimeToLive(Duration),
    TimeToIdle(Duration),
}

#[derive(Debug)]
struct Object {
    id: ObjectId,
    expires_at: Timestamp,
    policy: ExpirationPolicy,
}

#[derive(Debug, Default)]
struct ExpirationManager {
    expiration: BTreeSet<(Timestamp, ObjectId)>,
    objects: BTreeMap<ObjectId, Object>,
}

impl ExpirationManager {
    pub fn insert(&mut self, now: Timestamp, object_id: ObjectId, policy: ExpirationPolicy) {
        let time_to_live = match policy {
            ExpirationPolicy::TimeToLive(duration) => duration,
            ExpirationPolicy::TimeToIdle(duration) => duration,
        };

        let expires_at = now + time_to_live;

        self.expiration.insert((expires_at, object_id));
        self.objects.insert(
            object_id,
            Object {
                id: object_id,
                expires_at,
                policy,
            },
        );
    }

    pub fn access_object(&mut self, now: Timestamp, object_id: ObjectId) -> Option<()> {
        let object = self.objects.get_mut(&object_id)?;

        if object.expires_at < now {
            return None;
        }

        if let ExpirationPolicy::TimeToIdle(time_to_idle) = object.policy {
            self.expiration.remove(&(object.expires_at, object_id));
            object.expires_at = now + time_to_idle;
            self.expiration.insert((object.expires_at, object_id));
        }

        Some(())
    }

    pub fn collect_garbage(&mut self, now: Timestamp) -> usize {
        let live_threshold = (now, 0);
        // these are actually live, but we will swap, as `split_off` returns all items greater or equal the threshold
        let mut dead_objects = self.expiration.split_off(&live_threshold);
        std::mem::swap(&mut dead_objects, &mut self.expiration);

        let mut removed_objects = 0;
        for (_, object_id) in dead_objects {
            removed_objects += self.objects.remove(&object_id).is_some() as usize;
        }

        removed_objects
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_expired_objects() {
        let mut now = 0;
        let mut manager = ExpirationManager::default();

        manager.insert(now, 123, ExpirationPolicy::TimeToLive(5));
        manager.insert(now, 234, ExpirationPolicy::TimeToIdle(5));
        dbg!(&manager);

        assert_eq!(manager.access_object(now, 123), Some(()));
        assert_eq!(manager.access_object(now, 234), Some(()));

        now += 6;
        assert_eq!(manager.access_object(now, 123), None);
        assert_eq!(manager.access_object(now, 234), None);

        manager.collect_garbage(now);
        dbg!(&manager);
    }

    #[test]
    fn refreshes_tti_on_access() {
        let mut now = 0;
        let mut manager = ExpirationManager::default();

        manager.insert(now, 123, ExpirationPolicy::TimeToIdle(5));
        dbg!(&manager);

        now += 5;
        assert_eq!(manager.access_object(now, 123), Some(()));
        now += 5;
        assert_eq!(manager.access_object(now, 123), Some(()));
        dbg!(&manager);

        now += 6;
        assert_eq!(manager.access_object(now, 123), None);

        manager.collect_garbage(now);
        dbg!(&manager);
    }
}
