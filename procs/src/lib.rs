extern crate proc_macro;

mod ds;
mod endpoint;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn impl_number_ringslice(args: TokenStream, input: TokenStream) -> TokenStream {
    ds::impl_number_ringslice(args, input)
}

#[proc_macro]
pub fn topology_dispatcher(input: TokenStream) -> TokenStream {
    endpoint::topology_dispatcher(input)
}
