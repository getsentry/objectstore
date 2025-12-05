use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use objectstore_server::auth::Permission;
use serde_json::json;
use std::collections::HashSet;
use std::sync::LazyLock;

pub static TEST_EDDSA_KID: &str = "test_kid";

pub static TEST_EDDSA_PRIVKEY: LazyLock<String> = LazyLock::new(|| {
    let mut filepath = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
    filepath.extend(["config", "ed25519.private.pem"]);
    std::fs::read_to_string(&filepath).unwrap()
});

pub static TEST_EDDSA_PUBKEY: LazyLock<String> = LazyLock::new(|| {
    let mut filepath = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
    filepath.extend(["config", "ed25519.public.pem"]);
    std::fs::read_to_string(&filepath).unwrap()
});

#[derive(Default)]
pub struct TestTokenGenerator {
    org: Option<String>,
    project: Option<String>,
    usecase: Option<String>,
    permissions: HashSet<Permission>,
}

impl TestTokenGenerator {
    pub fn new() -> TestTokenGenerator {
        TestTokenGenerator::default()
    }

    pub fn org(mut self, org: &str) -> Self {
        self.org = Some(org.into());
        self
    }

    pub fn proj(mut self, proj: &str) -> Self {
        self.project = Some(proj.into());
        self
    }

    pub fn usecase(mut self, usecase: &str) -> Self {
        self.usecase = Some(usecase.into());
        self
    }

    pub fn perms(mut self, perms: &str) -> Self {
        let mut set = HashSet::new();
        if perms.contains("r") {
            set.insert(Permission::ObjectRead);
        }
        if perms.contains("w") {
            set.insert(Permission::ObjectWrite);
        }
        if perms.contains("d") {
            set.insert(Permission::ObjectDelete);
        }
        self.permissions = set;
        self
    }

    pub fn sign(self) -> String {
        let res = if self.project.is_some() {
            json!({"org": self.org.unwrap(), "project": self.project.unwrap(), "os:usecase": self.usecase.unwrap()})
        } else {
            json!({"org": self.org.unwrap(), "os:usecase": self.usecase.unwrap()})
        };
        let claims = json!({
            "exp": get_current_timestamp() + 300,
            "res": res,
            "permissions": self.permissions,
        });

        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(TEST_EDDSA_KID.into());
        let encoding_key = EncodingKey::from_ed_pem(TEST_EDDSA_PRIVKEY.as_bytes()).unwrap();

        encode(&header, &claims, &encoding_key).unwrap()
    }
}
