use std::collections::BTreeMap;

pub trait KVInterface<K: Ord + 'static, V: 'static> {
    fn insert(&mut self, key: K, value: V);
    fn remove(&mut self, key: &K) -> bool;
    fn get_mut(&mut self, key: &K) -> Option<&mut V>;
    fn drain_prefix(&mut self, upper_bound: &K) -> impl Iterator<Item = (K, V)>;
}

impl<K: Ord + 'static, V: 'static> KVInterface<K, V> for BTreeMap<K, V> {
    fn insert(&mut self, key: K, value: V) {
        self.insert(key, value);
    }

    fn remove(&mut self, key: &K) -> bool {
        self.remove(key).is_some()
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.get_mut(key)
    }

    fn drain_prefix(&mut self, upper_bound: &K) -> impl Iterator<Item = (K, V)> {
        // these are actually live, but we will swap, as `split_off` returns all items greater or equal the threshold
        let mut dead_objects = self.split_off(upper_bound);
        std::mem::swap(&mut dead_objects, self);

        dead_objects.into_iter()
    }
}

pub type ObjectId = usize;
pub type Timestamp = usize;
pub type Duration = usize;

#[derive(Debug)]
pub enum ExpirationPolicy {
    TimeToLive(Duration),
    TimeToIdle(Duration),
}

#[derive(Debug)]
pub struct Object {
    expires_at: Timestamp,
    policy: ExpirationPolicy,
}

#[derive(Debug)]
pub struct ExpirationManager<ExKV, ObjKV> {
    debounce: Duration,
    expiration: ExKV,
    objects: ObjKV,
}

impl<ExKV, ObjKV> ExpirationManager<ExKV, ObjKV>
where
    ExKV: KVInterface<(Timestamp, ObjectId), ()>,
    ObjKV: KVInterface<ObjectId, Object>,
{
    pub fn new(debounce: Duration, expiration: ExKV, objects: ObjKV) -> Self {
        Self {
            debounce,
            expiration,
            objects,
        }
    }

    pub fn insert(&mut self, now: Timestamp, object_id: ObjectId, policy: ExpirationPolicy) {
        let time_to_live = match policy {
            ExpirationPolicy::TimeToLive(duration) => duration,
            ExpirationPolicy::TimeToIdle(duration) => duration + self.debounce,
        };

        let expires_at = now + time_to_live;

        self.expiration.insert((expires_at, object_id), ());
        self.objects
            .insert(object_id, Object { expires_at, policy });
    }

    pub fn access_object(&mut self, now: Timestamp, object_id: ObjectId) -> Option<()> {
        let object = self.objects.get_mut(&object_id)?;

        if object.expires_at < now {
            return None;
        }

        if let ExpirationPolicy::TimeToIdle(time_to_idle) = object.policy {
            let time_alive = now.saturating_sub(
                object
                    .expires_at
                    .saturating_sub(time_to_idle)
                    .saturating_sub(self.debounce),
            );
            let needs_refresh = time_to_idle >= self.debounce || {
                let threshold = 1. - (time_alive as f64 / self.debounce as f64).powi(3);
                rand::random_bool(threshold)
            };
            if needs_refresh {
                self.expiration.remove(&(object.expires_at, object_id));
                object.expires_at = now + time_to_idle + self.debounce;
                self.expiration.insert((object.expires_at, object_id), ());
            }
        }

        Some(())
    }

    pub fn collect_garbage(&mut self, now: Timestamp) -> usize {
        let live_threshold = (now, 0);
        let dead_objects = self.expiration.drain_prefix(&live_threshold);

        let mut removed_objects = 0;
        for ((_, object_id), _) in dead_objects {
            removed_objects += self.objects.remove(&object_id) as usize;
        }

        removed_objects
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(
        debounce: Duration,
    ) -> ExpirationManager<BTreeMap<(Timestamp, ObjectId), ()>, BTreeMap<ObjectId, Object>> {
        ExpirationManager::new(debounce, Default::default(), Default::default())
    }

    #[test]
    fn removes_expired_objects() {
        let mut now = 0;
        let mut manager = manager(0);

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
        let mut manager = manager(2);

        manager.insert(now, 123, ExpirationPolicy::TimeToIdle(5));
        dbg!(&manager);

        now += 5;
        assert_eq!(manager.access_object(now, 123), Some(()));
        now += 5;
        assert_eq!(manager.access_object(now, 123), Some(()));
        dbg!(&manager);

        now += 8;
        assert_eq!(manager.access_object(now, 123), None);

        manager.collect_garbage(now);
        dbg!(&manager);
    }
}
