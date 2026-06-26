use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, ItemFn, parse};

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

    let entry_ident = syn::Ident::new(&format!("_ENTRY_{}", fn_name), fn_name.span());
    let register_ident = syn::Ident::new(&format!("_register_{}", fn_name), fn_name.span());

    let handler_struct_name = format!("{}Handler", fn_name.to_string().to_uppercase());
    let handler_struct_ident = syn::Ident::new(&handler_struct_name, fn_name.span());

    let pipeline_code = match &middleware {
        Some(mw_expr) => {
            if let Expr::Array(arr) = mw_expr {
                let with_calls = arr.elems.iter().map(|elem| {
                    quote! { pipeline = pipeline.with(#elem); }
                });
                quote! {{
                    let mut pipeline = Pipeline::new(handler);
                    #(#with_calls)*
                }}
            } else {
                quote! { Pipeline::new(handler).with(#mw_expr) }
            }
        }
        None => {
            quote! { Pipeline::new(handler) }
        }
    };

    let expanded = quote! {
        #input_fn

        struct #handler_struct_ident;

        #[async_trait::async_trait]
        impl event_base_core::handler::EHandler for #handler_struct_ident {
            async fn handle(&self, msg: &event_base_core::message::EMessage) -> event_base_core::handler::Ack {
                #fn_name(msg).await
            }
        }

        #[linkme::distributed_slice(event_base_core::registry::HANDLER_REGISTRY)]
        static #entry_ident: event_base_core::registry::HandlerEntry = event_base_core::registry::HandlerEntry {
            topic: #topic,
            register_fn: &#register_ident,
        };

        #pipeline_code

        async fn #register_ident(
            shutdown_tx: event_base_core::shutdown::ShutdownSender,
        ) -> Result<(), event_base_core::error::CoreError> {
            use event_base_core::topic::TopicRouter;
            use std::sync::Arc;

            let handler = Arc::new(#handler_struct_ident);
            let router = TopicRouter::global();
            let cr = ConsumerRouter::global();

            cr.register(&*#topic, handler).await?;
            router.register_topic(&*#topic).await?;


            for i in 0..#workers {
                let worker_id = format!("{}-{}", #topic, i);
                let shutdown_rx = shutdown_tx.subscribe();
                let worker = cr.create_worker(
                    #topic,
                    pipeline,
                    #timeout.map(std::time::Duration::from_secs),
                    #shutdown_timeout.map(std::time::Duration::from_secs),
                    #shutdown_check_interval.map(std::time::Duration::from_millis),
                    shutdown_rx,
                ).await?;
                info!("[WORKER]Worker {} start", worker_id);
            }

            Ok(())
        }
    };

    Ok(expanded.into())
}

#[derive(Debug, FromMeta, Default)]
pub struct HandlerArgs {
    pub topic: String,

    #[darling(default = "default_workers")]
    pub workers: usize,

    #[darling(default)]
    pub timeout: Option<u64>, // secs

    #[darling(default)]
    pub shutdown_timeout: Option<u64>, // secs

    #[darling(default)]
    pub shutdown_check_interval: Option<u64>, // millis

    #[darling(default)]
    pub middleware: Option<Expr>,
}

fn default_workers() -> usize {
    1
}

impl HandlerArgs {
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
