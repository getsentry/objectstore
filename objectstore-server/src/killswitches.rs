use std::collections::BTreeMap;

use objectstore_service::id::ObjectContext;
use serde::{Deserialize, Serialize};

/// A list of killswitches that may disable access to certain object contexts.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Killswitches(Vec<Killswitch>);

impl Killswitches {
    pub fn matches(&self, context: &ObjectContext) -> bool {
        self.0.iter().any(|s| s.matches(context))
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
}

impl Killswitch {
    pub fn matches(&self, context: &ObjectContext) -> bool {
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
        };

        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };

        assert!(switch.matches(&context));
    }

    #[test]
    fn test_matches_usecase() {
        let switch = Killswitch {
            usecase: Some("test".to_string()),
            scopes: BTreeMap::new(),
        };

        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };
        assert!(switch.matches(&context));

        // usecase differs
        let context = ObjectContext {
            usecase: "other".to_string(),
            scopes: Scopes::from_iter([Scope::create("any", "value").unwrap()]),
        };
        assert!(!switch.matches(&context));
    }

    #[test]
    fn test_matches_scopes() {
        let switch = Killswitch {
            usecase: None,
            scopes: BTreeMap::from([
                ("org".to_string(), "123".to_string()),
                ("project".to_string(), "456".to_string()),
            ]),
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
        assert!(switch.matches(&context));

        // project differs
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([
                Scope::create("org", "123").unwrap(),
                Scope::create("project", "999").unwrap(),
            ]),
        };
        assert!(!switch.matches(&context));

        // missing project
        let context = ObjectContext {
            usecase: "any".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!switch.matches(&context));
    }

    #[test]
    fn test_matches_full() {
        let switch = Killswitch {
            usecase: Some("test".to_string()),
            scopes: BTreeMap::from([("org".to_string(), "123".to_string())]),
        };

        // match
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(switch.matches(&context));

        // usecase differs
        let context = ObjectContext {
            usecase: "other".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };
        assert!(!switch.matches(&context));

        // scope differs
        let context = ObjectContext {
            usecase: "test".to_string(),
            scopes: Scopes::from_iter([Scope::create("org", "999").unwrap()]),
        };
        assert!(!switch.matches(&context));
    }
}
