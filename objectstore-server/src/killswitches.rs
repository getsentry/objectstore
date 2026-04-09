//! Runtime killswitches for disabling access to specific object contexts.
//!
//! A [`Killswitch`] matches requests by usecase, scope values, and optionally a downstream
//! service glob pattern. When any configured killswitch matches an incoming request, the server
//! rejects it immediately without forwarding to the storage backend.
//!
//! Killswitches are part of [`crate::config::Config`] and take effect on the next request after
//! a configuration reload — no server restart is required.

use std::cell::RefCell;
use std::num::NonZeroUsize;

use globset::{Glob, GlobMatcher};
use lru::LruCache;
use objectstore_options::Options;
use objectstore_service::id::ObjectContext;
use thread_local::ThreadLocal;

pub use objectstore_options::Killswitch;

/// A list of killswitches that may disable access to certain object contexts.
///
/// This serializes and deserializes directly from a list of killswitches.
#[derive(Debug, Default)]
pub struct Killswitches {
    /// The actual list of killswitches.
    switches: Vec<Killswitch>,

    /// Glob cache for fast matching of killswitches.
    cache: ThreadLocal<RefCell<LruCache<String, Option<GlobMatcher>>>>,
}

impl Killswitches {
    /// Creates a new `Killswitches` instance with the given killswitches.
    pub fn new(killswitches: Vec<Killswitch>) -> Self {
        Self {
            switches: killswitches,
            cache: ThreadLocal::new(),
        }
    }

    /// Returns `true` if any of the contained killswitches matches the given context.
    ///
    /// On match, emits a `server.request.killswitched` metric counter and a `warn!` log.
    pub fn matches(&self, context: &ObjectContext, service: Option<&str>) -> bool {
        let options = Options::get();

        let mut cache = self
            .cache
            .get_or(|| RefCell::new(LruCache::new(NonZeroUsize::MIN)))
            .borrow_mut();

        let total_count = self.switches.len() + options.killswitches().len();
        cache.resize(total_count.try_into().unwrap_or(NonZeroUsize::MIN));

        let Some(killswitch) = self
            .switches
            .iter()
            .chain(options.killswitches())
            .find(|s| matches(s, context, service, &mut cache))
        else {
            return false;
        };

        objectstore_metrics::count!("server.request.killswitched");
        objectstore_log::warn!(?killswitch, "Request rejected: killswitch active");
        true
    }

    /// Returns a slice of the contained killswitches.
    pub fn as_slice(&self) -> &[Killswitch] {
        &self.switches
    }
}

impl serde::Serialize for Killswitches {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.switches.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Killswitches {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::new(Vec::deserialize(deserializer)?))
    }
}

/// Returns `true` if this killswitch matches the given context and service.
fn matches(
    switch: &Killswitch,
    context: &ObjectContext,
    service: Option<&str>,
    cache: &mut LruCache<String, Option<GlobMatcher>>,
) -> bool {
    if let Some(ref switch_usecase) = switch.usecase
        && switch_usecase != &context.usecase
    {
        return false;
    }

    for (scope_name, scope_value) in &switch.scopes {
        match context.scopes.get_value(scope_name) {
            Some(value) if value == scope_value => (),
            _ => return false,
        }
    }

    // Check service pattern if specified
    if let Some(ref pattern) = switch.service {
        // If pattern is specified but no service header present, don't match
        let Some(service_value) = service else {
            return false;
        };

        let lookup = cache.get_or_insert_ref(pattern, || {
            Glob::new(pattern).ok().map(|g| g.compile_matcher())
        });

        match lookup {
            Some(m) if m.is_match(service_value) => (),
            _ => return false,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use objectstore_types::scope::{Scope, Scopes};

    use super::*;

    fn cache() -> LruCache<String, Option<GlobMatcher>> {
        LruCache::new(NonZeroUsize::MIN)
    }

    #[test]
    fn test_matches_empty() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: None,
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        assert!(matches(&switch, &context, None, &mut cache()));
    }

    #[test]
    fn test_matches_usecase() {
        let switch = Killswitch {
            usecase: Some("test".to_string()),
            scopes: BTreeMap::new(),
            service: None,
        };

        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };
        assert!(matches(&switch, &context, Some("anyservice"), &mut cache()));

        // usecase differs
        let context = ObjectContext {
            usecase: "other".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };
        assert!(!matches(
            &switch,
            &context,
            Some("anyservice"),
            &mut cache()
        ));
    }

    #[test]
    fn test_matches_scopes() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::from([
                ("org".to_string(), "123".to_string()),
                ("project".to_string(), "456".to_string()),
            ]),
            service: None,
        };

        // match, ignoring extra scope
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([
                Scope::create("org", "123").unwrap(),
                Scope::create("project", "456").unwrap(),
                Scope::create("extra", "789").unwrap(),
            ]),
        };
        assert!(matches(&switch, &context, Some("anyservice"), &mut cache()));

        // project differs
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([
                Scope::create("org", "123").unwrap(),
                Scope::create("project", "999").unwrap(),
            ]),
        };
        assert!(!matches(
            &switch,
            &context,
            Some("anyservice"),
            &mut cache()
        ));

        // missing project
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!matches(
            &switch,
            &context,
            Some("anyservice"),
            &mut cache()
        ));
    }

    #[test]
    fn test_matches_full() {
        let switch = Killswitch {
            usecase: Some("test".to_string()),
            scopes: BTreeMap::from([("org".to_string(), "123".to_string())]),
            service: Some("myservice-*".to_string()),
        };

        // match with all filters
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(matches(
            &switch,
            &context,
            Some("myservice-prod"),
            &mut cache()
        ));

        // usecase differs
        let context = ObjectContext {
            usecase: "other".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!matches(
            &switch,
            &context,
            Some("myservice-prod"),
            &mut cache()
        ));

        // scope differs
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "999").unwrap()]),
        };
        assert!(!matches(
            &switch,
            &context,
            Some("myservice-prod"),
            &mut cache()
        ));

        // service differs
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!matches(
            &switch,
            &context,
            Some("otherservice"),
            &mut cache()
        ));

        // missing service header
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!matches(&switch, &context, None, &mut cache()));
    }

    #[test]
    fn test_matches_service_exact() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: Some("myservice".to_string()),
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        assert!(matches(&switch, &context, Some("myservice"), &mut cache()));
        assert!(!matches(
            &switch,
            &context,
            Some("otherservice"),
            &mut cache()
        ));
        assert!(!matches(&switch, &context, None, &mut cache()));
    }

    #[test]
    fn test_matches_service_glob() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: Some("myservice-*".to_string()),
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        // Matches with glob pattern
        assert!(matches(
            &switch,
            &context,
            Some("myservice-prod"),
            &mut cache()
        ));
        assert!(matches(
            &switch,
            &context,
            Some("myservice-dev"),
            &mut cache()
        ));
        assert!(matches(
            &switch,
            &context,
            Some("myservice-staging"),
            &mut cache()
        ));

        // Doesn't match different service
        assert!(!matches(
            &switch,
            &context,
            Some("otherservice"),
            &mut cache()
        ));
        assert!(!matches(
            &switch,
            &context,
            Some("otherservice-prod"),
            &mut cache()
        ));

        // Doesn't match prefix without separator
        assert!(!matches(&switch, &context, Some("myservice"), &mut cache()));
    }

    #[test]
    fn test_matches_service_invalid_glob() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: Some("[invalid".to_string()), // Invalid glob pattern
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        // Invalid pattern should not match anything
        assert!(!matches(
            &switch,
            &context,
            Some("anyservice"),
            &mut cache()
        ));
        assert!(!matches(&switch, &context, Some("[invalid"), &mut cache()));
    }
}
