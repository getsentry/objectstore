//! Validation for usecase names shared by clients and the server.

/// Minimum length of a usecase name in bytes.
pub const MIN_USECASE_NAME_LENGTH: usize = 3;

/// Maximum length of a usecase name in bytes.
pub const MAX_USECASE_NAME_LENGTH: usize = 64;

/// An error indicating that a usecase name is invalid.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidUsecaseName {
    /// The name is outside the permitted length range.
    #[error("usecase name must be 3–64 ASCII characters long")]
    InvalidLength,
    /// The name contains a character outside the permitted set.
    #[error("invalid character {0:?} in usecase name; expected ASCII letters, digits, '_' or '-'")]
    InvalidChar(char),
}

/// Validates a usecase name.
///
/// Names must be 3–64 ASCII characters long and contain only letters (`A–Z`,
/// `a–z`), digits (`0–9`), underscores (`_`), and hyphens (`-`). Any permitted
/// character may appear in any position, including names consisting entirely of
/// underscores or hyphens. Names are case-sensitive and are never normalized.
///
/// # Errors
///
/// Returns [`InvalidUsecaseName`] if the byte length is outside the permitted
/// range or the name contains a forbidden character.
pub fn validate_usecase_name(name: &str) -> Result<(), InvalidUsecaseName> {
    if !(MIN_USECASE_NAME_LENGTH..=MAX_USECASE_NAME_LENGTH).contains(&name.len()) {
        return Err(InvalidUsecaseName::InvalidLength);
    }
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
            return Err(InvalidUsecaseName::InvalidChar(c));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_boundaries() {
        for len in [0, 1, 2, 3, 64, 65] {
            let result = validate_usecase_name(&"a".repeat(len));
            if (3..=64).contains(&len) {
                assert_eq!(result, Ok(()));
            } else {
                assert_eq!(result, Err(InvalidUsecaseName::InvalidLength));
            }
        }
    }

    #[test]
    fn allowed_characters() {
        for name in [
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-",
            "___",
            "---",
            "-a_",
        ] {
            assert_eq!(validate_usecase_name(name), Ok(()));
        }
    }

    #[test]
    fn forbidden_characters() {
        for c in [
            '/', '\\', '.', '%', '=', ';', '?', '#', ' ', '\t', '\n', '\0', 'é',
        ] {
            assert_eq!(
                validate_usecase_name(&format!("test{c}")),
                Err(InvalidUsecaseName::InvalidChar(c)),
            );
        }
        assert!(validate_usecase_name("testing/org=999/injected").is_err());
    }
}
