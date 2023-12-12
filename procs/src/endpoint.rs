use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, FnArg, ItemEnum, ItemTrait, PatType, Result, Token, TraitItem, Visibility,
    WhereClause,
};
pub fn topology_dispatcher(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as TopologyInput);

    let enum_def = input.enum_def;
    let enum_name = &enum_def.ident;
    let enum_generics = &enum_def.generics;
    let _enum_where = &enum_def.generics.where_clause;

    let trait_impls = input.traits.into_iter().map(|trait_def| {
        let where_clause = trait_def.where_clause;
        let trait_def = trait_def.trait_def;
        let trait_ident = &trait_def.ident;
        let methods = trait_def.items.iter().map(|item| {
            if let TraitItem::Method(method) = item {
                // 判断第一个参数是否是self
                let is_self = method.sig.inputs.iter().next().map(|arg| {
                    if let FnArg::Receiver(_) = arg {
                        true
                    } else {
                        // 判断当前方法是否有body
                        assert!(method.default.is_some(), "Trait method without self Receiver must have body");
                        false
                    }
                }).unwrap_or(false);
                is_self.then_some(method)
            } else {
                None
            }
        }).filter_map(|x| x);
        let type_def = trait_def.items.iter().find_map(|item| {
            if let TraitItem::Type(_item) = item {
                Some(quote!{
                    type Item = R;
                })
            } else {
                None
            }
        });
        let method_impls = methods.into_iter().map(|method| {
            let sig = &method.sig;
            let method_name = &sig.ident;
            let args = sig.inputs.iter().skip(1).map(|arg| if let FnArg::Typed(PatType { pat, .. }) = arg {
                pat
            } else {
                panic!("Only support typed arguments")
            });
            let arms = enum_def.variants.iter().map(|variant| {
                let args = args.clone();
                let variant_ident = &variant.ident;
                let variant_tuple = vec![format_ident!("p")];
                quote! {
                    #enum_name::#variant_ident(#(#variant_tuple),*) => #trait_ident::#method_name(#(#variant_tuple),*, #(#args),*),
                }
            });

            quote! {
                #[inline]
                #sig {
                    match self {
                        #(#arms)*
                    }
                }
            }
    });

     let trait_define = match trait_def.vis {
         Visibility::Public(_) => Some(&trait_def),
         _ => None,
     };

    quote! {
        #trait_define

        impl #enum_generics #trait_ident for #enum_name #enum_generics #where_clause {
            #type_def

            #(#method_impls)*
        }

    }
    });

    let try_from_arms = enum_def.variants.iter().map(|variant| {
        let variant_ident = &variant.ident;
        // 使用第一个单词，或者每个单词的首字母作为endpoint
        let mut endpoints = Vec::with_capacity(2);
        let s = variant_ident.to_string();
        // 1. 使用第一个单词。第二个大写字母之前的部分
        let l = s[1..].find(char::is_uppercase).map_or(s.len(), |i| i + 1);
        endpoints.push(s[..l].to_lowercase());
        // 2. 使用每个单词的首字母
        let caps: String = s.chars().filter(|c| c.is_uppercase()).collect();
        endpoints.push(caps.to_lowercase());
        // 3. 特殊处理
        if s.eq("PhantomService") {
            endpoints.push("pt".to_string());
        }

        quote! {
            #(#endpoints) | * => Ok(Self::#variant_ident(p.into())),
        }
    });
    let try_from = quote! {
        impl #enum_generics  #enum_name #enum_generics {
            pub fn try_from(p:P, endpoint:&str) -> std::io::Result<Self> {
                match endpoint {
                    #(#try_from_arms)*
                    _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
                }
            }
        }
    };

    let expanded = quote! {
        #enum_def

        #try_from

        #(#trait_impls)*

    };

    expanded.into()
}

struct Trait {
    trait_def: ItemTrait,
    where_clause: WhereClause, // 实现这个trait时，需要满足的条件
}

struct TopologyInput {
    enum_def: ItemEnum,
    traits: Vec<Trait>,
}

impl Parse for TopologyInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let enum_def: ItemEnum = input.parse()?;
        let mut traits = vec![];
        while !input.is_empty() {
            let trait_def: ItemTrait = input.parse()?;
            let _arrow: Token![=>] = input.parse()?;
            let where_clause: WhereClause = input.parse()?;
            traits.push(Trait {
                trait_def,
                where_clause,
            });
        }
        Ok(TopologyInput { enum_def, traits })
    }
}
