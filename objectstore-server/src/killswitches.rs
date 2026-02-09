use std::collections::BTreeMap;
use std::sync::OnceLock;

use globset::{Glob, GlobMatcher};
use objectstore_service::id::ObjectContext;
use serde::{Deserialize, Serialize};

/// A list of killswitches that may disable access to certain object contexts.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Killswitches(pub Vec<Killswitch>);

impl Killswitches {
    /// Returns `true` if any of the contained killswitches matches the given context.
    pub fn matches(&self, context: &ObjectContext, service: Option<&str>) -> bool {
        self.0.iter().any(|s| s.matches(context, service))
    }
}

/// A killswitch that may disable access to certain object contexts.
///
/// Note that at least one of the fields should be set, or else the killswitch will match all
/// contexts and discard all requests.
#[derive(Debug, Deserialize, Serialize)]
pub struct Killswitch {
    /// Optional usecase to match.
    ///
    /// If `None`, matches any usecase.
    #[serde(default)]
    pub usecase: Option<String>,

    /// Scopes to match.
    ///
    /// If empty, matches any scopes. Additional scopes in the context are ignored, so a killswitch
    /// matches if all of the specified scopes are present in the request with matching values.
    #[serde(default)]
    pub scopes: BTreeMap<String, String>,

    /// Optional service glob pattern to match.
    ///
    /// If `None`, matches any service (or absence of service header).
    /// If specified, the request must have a matching `x-downstream-service` header.
    #[serde(default)]
    pub service: Option<String>,

    /// Compiled glob matcher for the service pattern.
    ///
    /// This is lazily compiled on first use to avoid unwrap() calls and gracefully handle
    /// invalid patterns by treating them as non-matches.
    #[serde(skip)]
    #[serde(default)]
    pub service_matcher: OnceLock<Option<GlobMatcher>>,
}

impl PartialEq for Killswitch {
    fn eq(&self, other: &Self) -> bool {
        self.usecase == other.usecase
            && self.scopes == other.scopes
            && self.service == other.service
        // Skip service_matcher in comparison since it's derived from service
    }
}

impl Killswitch {
    /// Returns `true` if this killswitch matches the given context and service.
    pub fn matches(&self, context: &ObjectContext, service: Option<&str>) -> bool {
        if let Some(ref switch_usecase) = self.usecase
            && switch_usecase != &context.usecase
        {
            return false;
        }

        for (scope_name, scope_value) in &self.scopes {
            match context.scopes.get_value(scope_name) {
                Some(value) if value == scope_value => (),
                _ => return false,
            }
        }

        // Check service pattern if specified
        if let Some(ref pattern) = self.service {
            // If pattern is specified but no service header present, don't match
            let Some(service_value) = service else {
                return false;
            };

            let matcher = self
                .service_matcher
                .get_or_init(|| Glob::new(pattern).ok().map(|g| g.compile_matcher()));

            match matcher {
                Some(m) if m.is_match(service_value) => (),
                _ => return false,
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;

    #[test]
    fn test_matches_empty() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: None,
            service_matcher: OnceLock::new(),
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        assert!(switch.matches(&context, None));
    }

    #[test]
    fn test_matches_usecase() {
        let switch = Killswitch {
            usecase: Some("test".to_string()),
            scopes: BTreeMap::new(),
            service: None,
            service_matcher: OnceLock::new(),
        };

        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };
        assert!(switch.matches(&context, Some("anyservice")));

        // usecase differs
        let context = ObjectContext {
            usecase: "other".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };
        assert!(!switch.matches(&context, Some("anyservice")));
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
            service_matcher: OnceLock::new(),
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
        assert!(switch.matches(&context, Some("anyservice")));

        // project differs
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([
                Scope::create("org", "123").unwrap(),
                Scope::create("project", "999").unwrap(),
            ]),
        };
        assert!(!switch.matches(&context, Some("anyservice")));

        // missing project
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!switch.matches(&context, Some("anyservice")));
    }

    #[test]
    fn test_matches_full() {
        let switch = Killswitch {
            usecase: Some("test".to_string()),
            scopes: BTreeMap::from([("org".to_string(), "123".to_string())]),
            service: Some("myservice-*".to_string()),
            service_matcher: OnceLock::new(),
        };

        // match with all filters
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(switch.matches(&context, Some("myservice-prod")));

        // usecase differs
        let context = ObjectContext {
            usecase: "other".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!switch.matches(&context, Some("myservice-prod")));

        // scope differs
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "999").unwrap()]),
        };
        assert!(!switch.matches(&context, Some("myservice-prod")));

        // service differs
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!switch.matches(&context, Some("otherservice")));

        // missing service header
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!switch.matches(&context, None));
    }

    #[test]
    fn test_matches_service_exact() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: Some("myservice".to_string()),
            service_matcher: OnceLock::new(),
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        assert!(switch.matches(&context, Some("myservice")));
        assert!(!switch.matches(&context, Some("otherservice")));
        assert!(!switch.matches(&context, None));
    }

    #[test]
    fn test_matches_service_glob() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: Some("myservice-*".to_string()),
            service_matcher: OnceLock::new(),
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        // Matches with glob pattern
        assert!(switch.matches(&context, Some("myservice-prod")));
        assert!(switch.matches(&context, Some("myservice-dev")));
        assert!(switch.matches(&context, Some("myservice-staging")));

        // Doesn't match different service
        assert!(!switch.matches(&context, Some("otherservice")));
        assert!(!switch.matches(&context, Some("otherservice-prod")));

        // Doesn't match prefix without separator
        assert!(!switch.matches(&context, Some("myservice")));
    }

    #[test]
    fn test_matches_service_invalid_glob() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::new(),
            service: Some("[invalid".to_string()), // Invalid glob pattern
            service_matcher: OnceLock::new(),
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        // Invalid pattern should not match anything
        assert!(!switch.matches(&context, Some("anyservice")));
        assert!(!switch.matches(&context, Some("[invalid")));
    }
}
