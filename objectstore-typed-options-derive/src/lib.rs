//! Derive macro for [`objectstore_typed_options::SentryOptions`].
//!
//! See the [`objectstore-typed-options`] crate for full documentation and usage examples.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Fields, LitStr, parse_macro_input};

/// Derives the `SentryOptions` trait and generates runtime option machinery.
///
/// # Container attributes
///
/// - `namespace` — the sentry-options namespace (e.g. `"objectstore"`)
/// - `path` — relative path to the `sentry-options/` directory; the schema is resolved as
///   `{path}/schemas/{namespace}/schema.json`
///
/// # Generated code
///
/// For each struct field, `deserialize` calls `Deserialize::deserialize(options.get(NAMESPACE, "<field>")?)`.
///
/// Additionally generates:
/// - `SentryOptions` trait impl with `NAMESPACE`, `SCHEMA`, and `deserialize`
/// - Inherent `get() -> Arc<Self>`, `init() -> Result<(), Error>`, and (under `testing` feature)
///   `override_with()`
/// - A module-scoped `OnceLock<ArcSwap<T>>` static for the singleton instance
#[proc_macro_derive(SentryOptions, attributes(sentry_options))]
pub fn derive_sentry_options(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct Attrs {
    namespace: LitStr,
    path: LitStr,
}

fn parse_attrs(input: &DeriveInput) -> syn::Result<Attrs> {
    let mut namespace: Option<LitStr> = None;
    let mut path: Option<LitStr> = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("sentry_options") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("namespace") {
                let value = meta.value()?;
                namespace = Some(value.parse::<LitStr>()?);
                Ok(())
            } else if meta.path.is_ident("path") {
                let value = meta.value()?;
                path = Some(value.parse::<LitStr>()?);
                Ok(())
            } else {
                Err(meta.error("unknown sentry_options attribute"))
            }
        })?;
    }

    let namespace = namespace
        .ok_or_else(|| syn::Error::new(input.ident.span(), "missing `namespace` attribute"))?;
    let path =
        path.ok_or_else(|| syn::Error::new(input.ident.span(), "missing `path` attribute"))?;

    Ok(Attrs { namespace, path })
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let attrs = parse_attrs(&input)?;
    let name = &input.ident;

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new(
                    name.span(),
                    "SentryOptions can only be derived on structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new(
                name.span(),
                "SentryOptions can only be derived on structs",
            ));
        }
    };

    let namespace_str = &attrs.namespace;
    let path_str = &attrs.path;

    // Build deserialize body: one line per field.
    let field_deserializations: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().expect("named field");
            let field_key = field_name.to_string();
            quote! {
                #field_name: ::objectstore_typed_options::serde::Deserialize::deserialize(
                    options.get(Self::NAMESPACE, #field_key)?
                )?
            }
        })
        .collect();

    Ok(quote! {
        static __OPTIONS: ::std::sync::OnceLock<
            ::objectstore_typed_options::arc_swap::ArcSwap<#name>
        > = ::std::sync::OnceLock::new();

        impl ::objectstore_typed_options::SentryOptions for #name {
            const NAMESPACE: &str = #namespace_str;
            const SCHEMA: &str = include_str!(
                concat!(#path_str, "/schemas/", #namespace_str, "/schema.json")
            );

            fn deserialize(
                options: &::objectstore_typed_options::sentry_options::Options,
            ) -> ::std::result::Result<Self, ::objectstore_typed_options::Error> {
                ::std::result::Result::Ok(Self {
                    #(#field_deserializations),*
                })
            }
        }

        impl #name {
            /// Returns a snapshot of the current options.
            ///
            /// The returned [`Arc`] holds the most recently loaded values. Callers may hold
            /// it across await points without blocking updates — a new snapshot is swapped in
            /// atomically by the background refresh task without invalidating existing
            /// references.
            ///
            /// # Panics
            ///
            /// Panics if [`init`](Self::init) has not been called.
            #[cfg(not(feature = "testing"))]
            pub fn get() -> ::std::sync::Arc<Self> {
                __OPTIONS
                    .get()
                    .expect("options not initialized")
                    .load_full()
            }

            /// Returns a snapshot of the current options, deserializing fresh from schema
            /// defaults.
            ///
            /// In test builds this bypasses the global instance and reads directly from the
            /// schema, so [`init`](Self::init) does not need to be called. Use
            /// [`override_with`](Self::override_with) to test non-default values.
            #[cfg(feature = "testing")]
            pub fn get() -> ::std::sync::Arc<Self> {
                use ::objectstore_typed_options::SentryOptions as _;

                let inner = ::objectstore_typed_options::sentry_options::Options::from_schemas(
                    &[(Self::NAMESPACE, Self::SCHEMA)],
                )
                .expect("options schema should be valid");

                ::std::sync::Arc::new(
                    Self::deserialize(&inner).expect("failed to deserialize options"),
                )
            }

            /// Initializes the global options instance and spawns a background refresh task.
            ///
            /// The standard fallback chain is used:
            ///
            /// 1. `SENTRY_OPTIONS_DIR` environment variable
            /// 2. `/etc/sentry-options` (if it exists)
            /// 3. `sentry-options/` relative to the current working directory
            /// 4. Schema defaults (if no values file is present)
            ///
            /// Idempotent: if already initialized, returns `Ok(())` without re-loading.
            ///
            /// Must be called from within a Tokio runtime.
            pub fn init() -> ::std::result::Result<(), ::objectstore_typed_options::Error> {
                use ::objectstore_typed_options::SentryOptions as _;

                if __OPTIONS.get().is_none() {
                    let inner =
                        ::objectstore_typed_options::sentry_options::Options::from_schemas(
                            &[(Self::NAMESPACE, Self::SCHEMA)],
                        )?;
                    let initial = Self::deserialize(&inner)?;

                    if __OPTIONS
                        .set(::objectstore_typed_options::arc_swap::ArcSwap::from_pointee(
                            initial,
                        ))
                        .is_ok()
                    {
                        ::objectstore_typed_options::tokio::spawn(
                            ::objectstore_typed_options::refresh::<Self>(&__OPTIONS, inner),
                        );
                    }
                }

                ::std::result::Result::Ok(())
            }

            /// Overrides the global options for testing purposes.
            ///
            /// This function is only available in test builds and allows temporarily
            /// overriding specific options. The overrides are applied for the duration of
            /// the returned [`OverrideGuard`](objectstore_typed_options::sentry_options::testing::OverrideGuard).
            #[cfg(feature = "testing")]
            pub fn override_with(
                overrides: &[(&str, ::objectstore_typed_options::serde_json::Value)],
            ) -> ::objectstore_typed_options::sentry_options::testing::OverrideGuard {
                use ::objectstore_typed_options::SentryOptions as _;

                let overrides = overrides
                    .iter()
                    .map(|(key, value)| (Self::NAMESPACE, *key, value.clone()))
                    .collect::<::std::vec::Vec<_>>();

                ::objectstore_typed_options::sentry_options::testing::override_options(&overrides)
                    .expect("failed to override options")
            }
        }
    })
}
