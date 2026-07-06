//! Implementation of the `#[handler]` attribute macro.
//!
//! This module parses the attribute arguments and generates the required
//! boilerplate for handler registration and worker creation.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, ItemFn, Token, parse};

/// Parsed arguments for the `#[handler]` attribute.
struct HandlerArgsParsed {
    topic: String,
    workers: usize,
    timeout: Option<u64>,
    shutdown_timeout: Option<u64>,
    shutdown_check_interval: Option<u64>,
    middleware: Option<Expr>,
}

impl Parse for HandlerArgsParsed {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut topic = None;
        let mut workers = 1usize;
        let mut timeout: Option<u64> = None;
        let mut shutdown_timeout: Option<u64> = None;
        let mut shutdown_check_interval: Option<u64> = None;
        let mut middleware: Option<Expr> = None;

        while !input.is_empty() {
            let name: syn::Ident = input.parse()?;
            let _: Token![=] = input.parse()?;

            if name == "topic" {
                let lit: syn::LitStr = input.parse()?;
                topic = Some(lit.value());
            } else if name == "workers" {
                let lit: syn::LitInt = input.parse()?;
                workers = lit.base10_parse()?;
            } else if name == "timeout" {
                let lit: syn::LitInt = input.parse()?;
                timeout = Some(lit.base10_parse()?);
            } else if name == "shutdown_timeout" {
                let lit: syn::LitInt = input.parse()?;
                shutdown_timeout = Some(lit.base10_parse()?);
            } else if name == "shutdown_check_interval" {
                let lit: syn::LitInt = input.parse()?;
                shutdown_check_interval = Some(lit.base10_parse()?);
            } else if name == "middleware" {
                let mw: Expr = input.parse()?;
                middleware = Some(mw);
            } else {
                return Err(syn::Error::new(name.span(), format!("unknown argument: {}", name)));
            }

            // Skip optional comma
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }

        let topic = topic.ok_or_else(|| input.error("required argument `topic` is missing"))?;

        Ok(HandlerArgsParsed {
            topic,
            workers,
            timeout,
            shutdown_timeout,
            shutdown_check_interval,
            middleware,
        })
    }
}

/// The main implementation of the `#[handler]` macro.
///
/// It parses the function and the attribute arguments, then generates:
/// - The handler struct and `EHandler` impl.
/// - The static registry entry.
/// - The registration function that creates workers and pipelines.
pub fn handler_impl(args: TokenStream, input: TokenStream) -> Result<TokenStream, syn::Error> {
    let input_fn: ItemFn = parse(input)?;
    let args: HandlerArgsParsed = syn::parse(args)?;

    let fn_name = &input_fn.sig.ident;
    let topic = &args.topic;
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
    // Uses `Box::new(#handler_struct_ident)` instead of `handler` to avoid
    // forward-reference issues and type mismatch (Pipeline::new expects Box).
    let pipeline_code = match &middleware {
        Some(mw_expr) => {
            if let Expr::Array(arr) = mw_expr {
                // If an array is provided, chain multiple middlewares.
                let with_calls = arr.elems.iter().map(|elem| {
                    quote! { p = p.with(#elem); }
                });
                quote! {{
                    let mut p = Pipeline::new(Box::new(#handler_struct_ident));
                    #(#with_calls)*
                    p
                }}
            } else {
                // Single middleware.
                quote! { Pipeline::new(Box::new(#handler_struct_ident)).with(#mw_expr) }
            }
        }
        None => {
            // No middleware.
            quote! { Pipeline::new(Box::new(#handler_struct_ident)) }
        }
    };

    let timeout_expr = match timeout {
        Some(t) => quote! { Some(std::time::Duration::from_secs(#t)) },
        None => quote! { None },
    };
    let shutdown_timeout_expr = match shutdown_timeout {
        Some(t) => quote! { Some(std::time::Duration::from_secs(#t)) },
        None => quote! { None },
    };
    let shutdown_check_expr = match shutdown_check_interval {
        Some(t) => quote! { Some(std::time::Duration::from_millis(#t)) },
        None => quote! { None },
    };

    let expanded = quote! {
        #input_fn

        #[allow(non_camel_case_types)]
        struct #handler_struct_ident;

        #[async_trait::async_trait]
        impl ::event_base::core::handler::EHandler for #handler_struct_ident {
            async fn handler(&self, msg: &::event_base::core::message::EMessage) -> ::event_base::core::handler::Ack {
                #fn_name(msg).await
            }
        }

        #[linkme::distributed_slice(::event_base::core::registry::HANDLER_REGISTRY)]
        static #entry_ident: ::event_base::core::registry::HandlerEntry = ::event_base::core::registry::HandlerEntry {
            topic: #topic,
            register_fn: &#register_ident,
        };

        fn #register_ident(
            _shutdown_tx: ::event_base::core::shutdown::ShutdownSender,
        ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::std::result::Result<(), ::event_base::core::error::CoreError>> + Send>> {
            Box::pin(async move {
                use ::event_base::core::topic::TopicRouter;
                use ::event_base::core::queues::consumer_router::ConsumerRouter;
                use ::event_base::core::middleware::Pipeline;
                use std::sync::Arc;

                let handler = Arc::new(#handler_struct_ident);
                let router = TopicRouter::global();
                let cr = ConsumerRouter::global();
                cr.register(&*#topic, handler).await?;
                router.register_topic(&*#topic).await;
                let pipeline = Arc::new(#pipeline_code);
                for _i in 0..#workers {
                    let _worker = cr.create_worker(
                        #topic, pipeline.clone(),
                        #timeout_expr, #shutdown_timeout_expr, #shutdown_check_expr,
                    ).await?;
                }
                Ok(())
            })
        }
    };
    Ok(expanded.into())
}