use std::fmt;

use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;

/// Log output format.
///
/// Controls how log messages are formatted. The format can be explicitly specified or
/// auto-detected based on whether output is to a TTY.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Auto detect the best format.
    ///
    /// This chooses [`LogFormat::Pretty`] for TTY, otherwise [`LogFormat::Simplified`].
    Auto,

    /// Pretty printing with colors.
    ///
    /// ```text
    ///  INFO  objectstore::http > objectstore starting
    /// ```
    Pretty,

    /// Simplified plain text output.
    ///
    /// ```text
    /// 2020-12-04T12:10:32Z [objectstore::http] INFO: objectstore starting
    /// ```
    Simplified,

    /// Dump out JSON lines.
    ///
    /// ```text
    /// {"timestamp":"2020-12-04T12:11:08.729716Z","level":"INFO","logger":"objectstore::http","message":"objectstore starting","module_path":"objectstore::http","filename":"objectstore_service/src/http.rs","lineno":31}
    /// ```
    Json,
}

/// The logging format parse error.
#[derive(Clone, Debug)]
pub struct FormatParseError(String);

impl fmt::Display for FormatParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"error parsing "{}" as format: expected one of "auto", "pretty", "simplified", "json""#,
            self.0
        )
    }
}

impl std::str::FromStr for LogFormat {
    type Err = FormatParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "" => LogFormat::Auto,
            s if s.eq_ignore_ascii_case("auto") => LogFormat::Auto,
            s if s.eq_ignore_ascii_case("pretty") => LogFormat::Pretty,
            s if s.eq_ignore_ascii_case("simplified") => LogFormat::Simplified,
            s if s.eq_ignore_ascii_case("json") => LogFormat::Json,
            s => return Err(FormatParseError(s.into())),
        };

        Ok(result)
    }
}

impl std::error::Error for FormatParseError {}

mod display_fromstr {
    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: std::fmt::Display,
    {
        serializer.collect_str(&value)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        use serde::Deserialize;
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Logging configuration.
///
/// Controls the verbosity and format of log output. Logs are always written to stderr.
#[derive(Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    /// Minimum log level to output.
    ///
    /// Controls which log messages are emitted based on their severity. Messages at or above this
    /// level will be output. Valid levels in increasing severity: TRACE, DEBUG, INFO, WARN, ERROR,
    /// OFF.
    ///
    /// The `RUST_LOG` environment variable provides more granular control per module if needed.
    ///
    /// **Important**: Levels `DEBUG` and `TRACE` are very verbose and can impact performance; use
    /// only for debugging.
    ///
    /// # Default
    ///
    /// `INFO`
    ///
    /// # Environment Variable
    ///
    /// `OS__LOGGING__LEVEL`
    ///
    /// # Considerations
    ///
    /// - `TRACE` and `DEBUG` can be very verbose and impact performance; use only for debugging
    /// - `INFO` is appropriate for production
    /// - `WARN` or `ERROR` can be used to reduce log volume in high-traffic systems
    #[serde(with = "display_fromstr")]
    pub level: LevelFilter,

    /// Log output format.
    ///
    /// Determines how log messages are formatted. See [`LogFormat`] for available options and
    /// examples.
    ///
    /// # Default
    ///
    /// `Auto` (pretty for TTY, simplified otherwise)
    ///
    /// # Environment Variable
    ///
    /// `OS__LOGGING__FORMAT`
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LevelFilter::INFO,
            format: LogFormat::Auto,
        }
    }
}
