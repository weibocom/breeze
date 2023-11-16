extern crate proc_macro;

mod deref;
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

#[proc_macro_attribute]
pub fn dispatcher_trait_deref(attr: TokenStream, input: TokenStream) -> TokenStream {
    deref::impl_trait_for_deref_target(attr, input)
}
