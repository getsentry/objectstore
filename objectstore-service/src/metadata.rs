use std::fmt::{self, Display};
use std::str::FromStr;

use uuid::Uuid;
use watto::Pod;

/// A service-defined key for an object.
///
/// The current `version: 0` of the key has a size of 40 bytes, and primarily contains a [`Uuid`].
/// It has enough reserved bytes to also carry the scope if we decide to do so.
///
/// These keys are encoded as 64 "base32"-encoded string.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ObjectKey {
    /// The key version.
    ///
    /// This should be increased whenever the format of the key changes substantially in
    /// a non-backwards/-forwards way.
    version: u8,

    /// Denotes the backend being used to store this object.
    ///
    /// I could imagine using `backend: 0` as a special case of "extremely small file optimization",
    /// where the whole object is stored within the key.
    /// Otherwise, backends start at `1`, and are hardcoded at first, possibly making them configurable
    /// at a later point, though care must be taken to do that in a backwards compatible way.
    pub backend: u8,

    /// This is just some bytes reserved for more metadata.
    ///
    /// We might want to store things like the usecase (hardcoded), org-id and project-id here.
    /// Giving those IDs are both 8-bytes, we have enough space here do to so.
    _reserved: [u8; 22],

    /// The [`Uuid`] of the object.
    pub uuid: uuid::Bytes, // this is an alias for [u8;16]
}

const _: () = const {
    assert!(std::mem::align_of::<ObjectKey>() == 1);
    assert!(std::mem::size_of::<ObjectKey>() == 40);
};

impl ObjectKey {
    /// Generates a fresh [`ObjectKey`], tagging it with the given `backend`.
    pub fn for_backend(backend: u8) -> Self {
        Self {
            version: 0,
            backend,
            _reserved: Default::default(),
            uuid: Uuid::now_v7().into_bytes(),
        }
    }
}

unsafe impl Pod for ObjectKey {}

impl Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        data_encoding::BASE32HEX_NOPAD
            .encode_display(self.as_bytes())
            .fmt(f)
    }
}

impl FromStr for ObjectKey {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        const KEY_SIZE: usize = std::mem::size_of::<ObjectKey>();
        let mut buf = [0; KEY_SIZE];
        let read_len = data_encoding::BASE32HEX_NOPAD
            .decode_mut(s.as_bytes(), &mut buf)
            .map_err(|e| std::io::Error::other(e.error))?;

        if read_len != KEY_SIZE {
            return Err(std::io::Error::other("invalid key length"));
        }
        let key = ObjectKey::ref_from_bytes(&buf)
            .ok_or_else(|| std::io::Error::other("invalid key length"))?;
        // TODO: actually verify version, etc

        Ok(*key)
    }
}

/// The fully scoped object key
///
/// This consists of a usecase, the scope, and the object key.
#[derive(Debug)]
pub struct ScopedKey {
    /// The usecase, or "product" this object belongs to.
    ///
    /// This can be defined on-the-fly by the client, but special server logic
    /// (such as the concrete backend/bucket) can be tied to this as well.
    pub usecase: String,

    /// The scope of the object, used for compartmentalization.
    pub scope: String,

    /// This is the storage key of the object, unique within the usecase/scope.
    pub key: ObjectKey,
}

impl ScopedKey {
    /// Creates a new [`ScopedKey`] from its constituent parts.
    pub fn from_parts(usecase: String, scope: String, key: &str) -> anyhow::Result<ScopedKey> {
        let key = ObjectKey::from_str(key)?;
        Ok(ScopedKey {
            usecase,
            scope,
            key,
        })
    }

    /// Formats the key as a path.
    pub fn as_path(&self) -> ScopedKeyPath<'_> {
        ScopedKeyPath(self)
    }
}

/// A wrapper struct implementing [`Display`], formatting a key as a path.
#[derive(Debug)]
pub struct ScopedKeyPath<'a>(&'a ScopedKey);

impl Display for ScopedKeyPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.0.usecase, self.0.scope, self.0.key)
    }
}
