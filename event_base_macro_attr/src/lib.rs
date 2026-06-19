use proc_macro::TokenStream;

mod handler;

#[proc_macro_attribute]
pub fn handler(args: TokenStream, input: TokenStream) -> TokenStream {
    handler::handler_impl(args, input)
        .unwrap_or_else(|e| e.to_compile_error().into())
}