/// Logs a message at a given static level.
///
/// The first argument must be a [`tracing::Level`] constant. An optional `!!<error>` second
/// argument casts the expression to `&dyn std::error::Error` and records it as the `error` field.
/// All remaining arguments are forwarded verbatim to [`tracing::event!`].
///
/// The level-specific macros (`error!`, `warn!`, `info!`, `debug!`, `trace!`) are thin wrappers
/// around this macro and are the preferred call form.
///
/// # Examples
///
/// ```rust
/// # use objectstore_log::Level;
/// # fn example() -> anyhow::Result<()> {
/// let err = anyhow::anyhow!("something broke");
/// objectstore_log::event!(Level::ERROR, !!err.as_ref(), "fatal error");
/// objectstore_log::event!(Level::WARN, field = "value", "plain event");
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! event {
    ($level:expr, !!$error:expr $(,)?) => {
        $crate::tracing::event!($level, error = $error as &dyn ::std::error::Error)
    };
    ($level:expr, !!$error:expr, $($args:tt)+) => {
        $crate::tracing::event!($level, error = $error as &dyn ::std::error::Error, $($args)+)
    };
    ($level:expr, $($args:tt)*) => {
        $crate::tracing::event!($level, $($args)*)
    };
}

/// Logs a message at `ERROR` level. See [`event!`] for full syntax.
#[macro_export]
macro_rules! error {
    ($($args:tt)*) => { $crate::event!($crate::tracing::Level::ERROR, $($args)*) };
}

/// Logs a message at `WARN` level. See [`event!`] for full syntax.
#[macro_export]
macro_rules! warn {
    ($($args:tt)*) => { $crate::event!($crate::tracing::Level::WARN, $($args)*) };
}

/// Logs a message at `INFO` level. See [`event!`] for full syntax.
#[macro_export]
macro_rules! info {
    ($($args:tt)*) => { $crate::event!($crate::tracing::Level::INFO, $($args)*) };
}

/// Logs a message at `DEBUG` level. See [`event!`] for full syntax.
#[macro_export]
macro_rules! debug {
    ($($args:tt)*) => { $crate::event!($crate::tracing::Level::DEBUG, $($args)*) };
}

/// Logs a message at `TRACE` level. See [`event!`] for full syntax.
#[macro_export]
macro_rules! trace {
    ($($args:tt)*) => { $crate::event!($crate::tracing::Level::TRACE, $($args)*) };
}

/// Dispatches a log event at a level determined at runtime.
///
/// The first argument is a [`tracing::Level`] value resolved at runtime; all remaining arguments
/// (including an optional `!!<error>`) are forwarded to the appropriate level macro.
///
/// Prefer the level-specific macros (`error!`, `warn!`, …) when the level is known at compile
/// time. Use this macro only when the level is dynamic.
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
