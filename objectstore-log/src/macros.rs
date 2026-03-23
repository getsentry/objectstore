/// Logs an error at `ERROR` level, casting the expression to `&dyn std::error::Error`
/// automatically.
///
/// The error is recorded as the `error` structured field. Any additional tracing key-value fields
/// or a log message may follow after the error expression.
///
/// # Examples
///
/// ```rust
/// # fn example() -> anyhow::Result<()> {
/// let err = anyhow::anyhow!("something broke");
/// objectstore_log::exception!(err.as_ref());
/// objectstore_log::exception!(err.as_ref(), "fatal startup error");
/// objectstore_log::exception!(err.as_ref(), component = "storage", "write failed");
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! exception {
    ($error:expr $(,)?) => {
        $crate::error!(error = $error as &dyn ::std::error::Error)
    };
    ($error:expr, $($args:tt)*) => {
        $crate::error!(error = $error as &dyn ::std::error::Error, $($args)*)
    };
}

/// Logs an error at `WARN` level, casting the expression to `&dyn std::error::Error`
/// automatically.
///
/// Identical to [`exception!`] but uses `warn!` instead of `error!`. Use for recoverable
/// errors where the operation can continue (e.g. retries).
///
/// # Examples
///
/// ```rust
/// # fn example() -> anyhow::Result<()> {
/// let err = anyhow::anyhow!("transient failure");
/// objectstore_log::warn_exception!(err.as_ref(), "retrying request");
/// objectstore_log::warn_exception!(err.as_ref(), attempt = 2, "retry");
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! warn_exception {
    ($error:expr $(,)?) => {
        $crate::warn!(error = $error as &dyn ::std::error::Error)
    };
    ($error:expr, $($args:tt)*) => {
        $crate::warn!(error = $error as &dyn ::std::error::Error, $($args)*)
    };
}

/// Dispatches a log event at a level determined at runtime.
///
/// The first argument is a [`Level`] expression; all remaining arguments are forwarded to the
/// appropriate level macro (`trace!`, `debug!`, `info!`, `warn!`, or `error!`).
///
/// # Examples
///
/// ```rust
/// # use objectstore_log::Level;
/// let level = Level::WARN;
/// objectstore_log::event_dyn!(level, "something happened");
/// objectstore_log::event_dyn!(level, key = "value", "with a field");
/// ```
#[macro_export]
macro_rules! event_dyn {
    ($level:expr, $($args:tt)*) => {
        match $level {
            $crate::tracing::Level::ERROR => $crate::error!($($args)*),
            $crate::tracing::Level::WARN  => $crate::warn!($($args)*),
            $crate::tracing::Level::INFO  => $crate::info!($($args)*),
            $crate::tracing::Level::DEBUG => $crate::debug!($($args)*),
            $crate::tracing::Level::TRACE => $crate::trace!($($args)*),
        }
    };
}
