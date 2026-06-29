//! Implementation of the `#[handler]` attribute macro.
//!
//! This module parses the attribute arguments and generates the required
//! boilerplate for handler registration and worker creation.

use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, ItemFn, parse};

/// The main implementation of the `#[handler]` macro.
///
/// It parses the function and the attribute arguments, then generates:
/// - The handler struct and `EHandler` impl.
/// - The static registry entry.
/// - The registration function that creates workers and pipelines.
pub fn handler_impl(args: TokenStream, input: TokenStream) -> Result<TokenStream, syn::Error> {
    let input_fn: ItemFn = parse(input)?;
    let args = HandlerArgs::from_attr_args(args)?;

    let fn_name = &input_fn.sig.ident;
    let topic = args.topic;
    let workers = args.workers;
    let timeout = args.timeout;
    let middleware = args.middleware;
    let shutdown_check_interval = args.shutdown_check_interval;
    let shutdown_timeout = args.shutdown_timeout;

    // Identifiers for generated static entries.
    let entry_ident = syn::Ident::new(&format!("_ENTRY_{}", fn_name), fn_name.span());
    let register_ident = syn::Ident::new(&format!("_register_{}", fn_name), fn_name.span());

    // Handler struct name (uppercased function name + "Handler").
    let handler_struct_name = format!("{}Handler", fn_name.to_string().to_uppercase());
    let handler_struct_ident = syn::Ident::new(&handler_struct_name, fn_name.span());

    // Generate the pipeline construction code based on middleware argument.
    let pipeline_code = match &middleware {
        Some(mw_expr) => {
            if let Expr::Array(arr) = mw_expr {
                // If an array is provided, chain multiple middlewares.
                let with_calls = arr.elems.iter().map(|elem| {
                    quote! { pipeline = pipeline.with(#elem); }
                });
                quote! {{
                    let mut pipeline = Pipeline::new(handler);
                    #(#with_calls)*
                }}
            } else {
                // Single middleware.
                quote! { Pipeline::new(handler).with(#mw_expr) }
            }
        }
        None => {
            // No middleware.
            quote! { Pipeline::new(handler) }
        }
    };

    let expanded = quote! {
        // The original function remains unchanged.
        #input_fn

        // The handler struct that wraps the function.
        struct #handler_struct_ident;

        #[async_trait::async_trait]
        impl event_base_core::handler::EHandler for #handler_struct_ident {
            async fn handler(&self, msg: &event_base_core::message::EMessage) -> event_base_core::handler::Ack {
                #fn_name(msg).await
            }
        }

        // Static entry in the distributed registry.
        #[linkme::distributed_slice(event_base_core::registry::HANDLER_REGISTRY)]
        static #entry_ident: event_base_core::registry::HandlerEntry = event_base_core::registry::HandlerEntry {
            topic: #topic,
            register_fn: &#register_ident,
        };

        // The registration function that sets up everything.
        async fn #register_ident(
            shutdown_tx: event_base_core::shutdown::ShutdownSender,
        ) -> Result<(), event_base_core::error::CoreError> {
            use event_base_core::topic::TopicRouter;
            use std::sync::Arc;

            #pipeline_code

            let handler = Arc::new(#handler_struct_ident);
            let router = TopicRouter::global();
            let cr = ConsumerRouter::global();

            cr.register(&*#topic, handler).await?;
            router.register_topic(&*#topic).await?;

            // Create the specified number of workers.
            for i in 0..#workers {
                let shutdown_rx = shutdown_tx.subscribe();
                let worker = cr.create_worker(
                    #topic,
                    pipeline,
                    #timeout.map(std::time::Duration::from_secs),
                    #shutdown_timeout.map(std::time::Duration::from_secs),
                    #shutdown_check_interval.map(std::time::Duration::from_millis),
                    shutdown_rx,
                ).await?;
            }

            Ok(())
        }
    };

    Ok(expanded.into())
}

/// Arguments parsed from the `#[handler]` attribute.
#[derive(Debug, FromMeta, Default)]
pub struct HandlerArgs {
    /// The topic this handler processes (required).
    pub topic: String,

    /// Number of worker tasks to spawn (default: 1).
    #[darling(default = "default_workers")]
    pub workers: usize,

    /// Processing timeout in seconds per message (optional).
    #[darling(default)]
    pub timeout: Option<u64>, // secs

    /// Shutdown timeout in seconds (optional).
    #[darling(default)]
    pub shutdown_timeout: Option<u64>, // secs

    /// Shutdown check interval in milliseconds (optional).
    #[darling(default)]
    pub shutdown_check_interval: Option<u64>, // millis

    /// Middleware expression (single or array) (optional).
    #[darling(default)]
    pub middleware: Option<Expr>,
}

/// Default number of workers.
fn default_workers() -> usize {
    1
}

impl HandlerArgs {
    /// Parses the attribute arguments from the `TokenStream`.
    pub fn from_attr_args(args: TokenStream) -> darling::Result<Self> {
        let attr_args = match NestedMeta::parse_meta_list(args.into()) {
            Ok(v) => v,
            Err(e) => {
                return Err(darling::Error::from(e));
            }
        };
        Self::from_list(&attr_args)
    }
}
