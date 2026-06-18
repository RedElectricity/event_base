use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, parse2, punctuated::Punctuated, ItemFn, LitInt, LitStr, Meta, Token, Expr, MetaNameValue};
use syn::parse::{Parse, ParseStream, Parser};

#[proc_macro_attribute]
pub fn handler(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let meta_list = parse_macro_input!(args with Punctuated::<Meta, Token![,]>::parse_terminated);

    let mut topic = String::new();
    let mut workers = 1;
    let mut timeout = None::<u64>;

    for meta in meta_list {
        if let Meta::NameValue(nv) = meta {
            let ident = nv.path.get_ident().unwrap().to_string();
            let expr = nv.value;

            match ident.as_str() {
                "topic" => {
                    if let Ok(lit) = parse2::<LitStr>(expr.to_token_stream()) {
                        topic = lit.value();
                    }
                }
                "workers" => {
                    if let Ok(lit) = parse2::<LitInt>(expr.to_token_stream()) {
                        workers = lit.base10_parse().unwrap();
                    }
                }
                "timeout" => {
                    if let Ok(lit) = parse2::<LitInt>(expr.to_token_stream()) {
                        timeout = Some(lit.base10_parse().unwrap());
                    }
                }
                _ => {}
            }
        }
    }

    if topic.is_empty() {
        return syn::Error::new_spanned(&input_fn, "topic is required")
            .to_compile_error()
            .into();
    }

    let fn_name = &input_fn.sig.ident;
    let handler_struct_name = format!("{}Handler", fn_name.to_string().to_uppercase());
    let handler_struct_ident = syn::Ident::new(&handler_struct_name, fn_name.span());

    let timeout_expr = match timeout {
        Some(secs) => quote! { Some(std::time::Duration::from_secs(#secs)) },
        None => quote! { None },
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

        /// Register this handler to Topic Router and create workers
        pub async fn register_ #fn_name(
            shutdown_tx: ShutdownSender,
        ) -> Result<(), CoreError> {
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
                    #timeout_expr,
                    shutdown_rx,
                ).await?;
                tokio::spawn(worker.start());
            }

            Ok(())
        }

        // 将注册函数加入分布式切片
        #[linkme::distributed_slice(event_base_core::registry::HANDLER_REGISTRY)]
        static __ENTRY_ #fn_name: event_base_core::registry::HandlerEntry = event_base_core::registry::HandlerEntry {
            topic: #topic,
            register_fn: &register_ #fn_name,
        };
    };

    TokenStream::from(expanded)
}

#[proc_macro]
pub fn send_msg(input: TokenStream) -> TokenStream {
    let parser = Punctuated::<Expr, Token![,]>::parse_terminated;
    let args = match parser.parse(input) {
        Ok(args) => args,
        Err(e) => return e.to_compile_error().into(),
    };

    if args.len() != 2 {
        let error = syn::Error::new_spanned(&args, "expected exactly 2 arguments: topic, msg");
        return error.to_compile_error().into();
    }

    let topic_expr = &args[0];
    let msg_expr = &args[1];

    let topic_str = if let Ok(lit) = parse2::<LitStr>(topic_expr.to_token_stream()) {
        lit.value()
    } else {
        return quote! {
            {
                let mut msg = #msg_expr;
                let topic_str: String = #topic_expr.to_string();
                msg.topic = event_base_core::message::Topic(topic_str.clone());
                event_base_core::topic::TopicRouter::global()
                    .send(&topic_str, msg)
            }
        }.into();
    };

    quote! {
        {
            let mut msg = #msg_expr;
            msg.topic = event_base_core::message::Topic(#topic_str.to_string());
            event_base_core::topic::TopicRouter::global()
                .send(#topic_str, msg)
        }
    }.into()
}

struct StartSystemArgs {
    factory: Expr,
    wal: Option<Expr>,
}

impl Parse for StartSystemArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // 期望格式: { factory: expr, wal: expr }
        // 先解析大括号内容
        let content;
        syn::braced!(content in input);
        let mut factory = None;
        let mut wal = None;

        // 解析以逗号分隔的 name: value 对
        let pairs = Punctuated::<MetaNameValue, Token![,]>::parse_terminated(&content)?;

        for pair in pairs {
            let name = pair.path.get_ident()
                .ok_or_else(|| syn::Error::new_spanned(&pair.path, "expected identifier"))?
                .to_string();
            match name.as_str() {
                "factory" => {
                    factory = Some(pair.value);
                }
                "wal" => {
                    wal = Some(pair.value);
                }
                _ => return Err(syn::Error::new_spanned(pair, format!("unknown parameter: {}", name))),
            }
        }

        let factory = factory.ok_or_else(|| syn::Error::new(input.span(), "missing `factory`"))?;
        Ok(StartSystemArgs { factory, wal })
    }
}

/// 启动事件系统
///
/// # Example
/// ```rust,ignore
/// let shutdown_tx = start_queue_system! {
///     factory: MemoryQueueFactory::new(100),
///     wal: Some(wal),
/// }?;
/// ```
#[proc_macro]
pub fn start_queue_system(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as StartSystemArgs);

    let factory = args.factory;
    let wal_expr = args.wal;

    // 生成 wal 初始化和启动调度器的代码
    let wal_init = match &wal_expr {
        Some(_) => quote! { let wal = #wal_expr; },
        None => quote! { let wal: Option<std::sync::Arc<tokio::sync::Mutex<dyn event_base_core::wal::Wal>>> = None; },
    };

    let wal_spawn = if wal_expr.is_some() {
        quote! {
            if let Some(wal) = &wal {
                let router = event_base_core::topic::TopicRouter::global();
                tokio::spawn(event_base_core::delay::run_scheduler(wal.clone(), router));
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {{
        use std::sync::Arc;
        use event_base_core::topic::TopicRouter;
        use event_base_core::shutdown::shutdown_channel;
        use event_base_core::registry::register_all_handlers;

        // 1. 初始化 TopicRouter
        let factory = Arc::new(#factory);
        #wal_init
        TopicRouter::init(factory, wal.clone())?;

        // 2. 创建 shutdown 通道
        let (shutdown_tx, _) = shutdown_channel();

        // 3. 自动注册所有 Handler（linkme 分布式切片）
        register_all_handlers(shutdown_tx.clone()).await?;

        // 4. 启动延迟消息调度器
        #wal_spawn

        // 5. 返回 shutdown_tx
        Result::<_, event_base_core::error::CoreError>::Ok(shutdown_tx)
    }};

    TokenStream::from(expanded)
}
