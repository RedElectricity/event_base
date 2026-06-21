use darling::ast::NestedMeta;
use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse, Expr, ItemFn};

pub fn handler_impl(args: TokenStream, input: TokenStream) -> Result<TokenStream, syn::Error> {
    let input_fn: ItemFn = parse(input)?;
    let args = HandlerArgs::from_attr_args(args)?;

    let fn_name = &input_fn.sig.ident;
    let topic = args.topic;
    let workers = args.workers;
    let timeout = args.timeout;
    let middleware = args.middleware;

    let entry_ident = syn::Ident::new(&format!("_ENTRY_{}", fn_name), fn_name.span());
    let register_ident = syn::Ident::new(&format!("_register_{}", fn_name), fn_name.span());

    let handler_struct_name = format!("{}Handler", fn_name.to_string().to_uppercase());
    let handler_struct_ident = syn::Ident::new(&handler_struct_name, fn_name.span());

    let pipeline_code = match &middleware {
        Some(mw_expr) => {
            if let Expr::Array(arr) = mw_expr {
                // 生成每个元素的 .with() 调用
                let with_calls = arr.elems.iter().map(|elem| {
                    quote! { pipeline = pipeline.with(#elem); }
                });
                quote! {{
                    let mut pipeline = Pipeline::new(handler);
                    #(#with_calls)*
                    pipeline.build()
                }}
            } else {
                // 单个中间件
                quote! { Pipeline::new(handler).with(#mw_expr).build() }
            }
        }
        None => {
            quote! { Pipeline::new(handler).build() }
        }
    };

    let expanded = quote! {
        #input_fn

        struct #handler_struct_ident;

        #[async_trait::async_trait]
        impl event_base_core::handler::Handler for #handler_struct_ident {
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

            router.register_topic(#topic, handler).await?;

            for i in 0..#workers {
                let worker_id = format!("{}-{}", #topic, i);
                let shutdown_rx = shutdown_tx.subscribe();
                let worker = router.create_worker(
                    #topic,
                    handler.clone(),
                    #timeout,
                    pipeline,
                    shutdown_rx,
                ).await?;
                tokio::spawn(worker.start());
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
    pub timeout: Option<u64>,

    // 直接用 Expr 存储，不解析内容
    #[darling(default)]
    pub middleware: Option<Expr>,

    // shutdown 也是 Expr，用户传什么就是什么
    #[darling(default)]
    pub shutdown: Option<Expr>,
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
