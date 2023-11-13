extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemTrait, PatType, TraitItem, TraitItemMethod};

// 在 trait 上添加 #[procs::dispatcher_trait_deref] 属性，会为trait生成一个impl块，该impl 块会为trait 的所有方法生成一个默认实现，
// 该实现会调用 trait 的 Deref::deref 方法
pub fn impl_trait_for_deref_target(_attr: TokenStream, input: TokenStream) -> TokenStream {
    // 解析 trait
    let trait_def = parse_macro_input!(input as ItemTrait);
    let original_trait_def = trait_def.clone();
    let trait_name = &trait_def.ident;
    let methods = trait_def.items.iter().filter_map(|item| {
        if let TraitItem::Method(method) = item {
            Some(method)
        } else {
            None
        }
    });

    // 生成方法实现
    let method_impls = methods.map(|method| {
        let TraitItemMethod { sig, .. } = method;
        let method_name = &sig.ident;
        let args = sig.inputs.iter().skip(1).map(|arg| {
            if let FnArg::Typed(PatType { pat, .. }) = arg {
                pat
            } else {
                panic!("unexpected argument");
            }
        });

        quote! {
            #sig {
                (&**self).#method_name(#(#args),*)
            }
        }
    });

    let types = trait_def.items.iter().find_map(|item| {
        if let TraitItem::Type(_ty) = item {
            Some(quote! { type Item = E::Item; })
        } else {
            None
        }
    });

    let supertraits = trait_def.supertraits.iter();
    let t_supertraits = supertraits.clone();

    // 生成 impl 块
    let expanded = quote! {
        #original_trait_def

        impl<T, E> #trait_name for T
        where
            T: std::ops::Deref<Target = E> + #( #t_supertraits + )*,
            E: #trait_name + #( #supertraits + )*,
        {
            #types

            #(#method_impls)*
        }
    };

    expanded.into()
}
