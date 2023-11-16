use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, token::Comma, AttributeArgs, FnArg, Ident,
    ItemTrait, NestedMeta, PatType, ReturnType, Type,
};

#[derive(Clone, Copy)]
enum Endianess {
    Big,
    Little,
}

impl<'a> From<&'a str> for Endianess {
    fn from(s: &'a str) -> Self {
        match s {
            "be" | "" => Endianess::Big,
            "le" => Endianess::Little,
            _ => panic!("invalid endianess"),
        }
    }
}

pub fn impl_number_ringslice(args: TokenStream, input: TokenStream) -> TokenStream {
    // 解析宏属性和trait定义
    let attr_args = parse_macro_input!(args as AttributeArgs);
    let input_trait = parse_macro_input!(input as ItemTrait);

    let default_endianess = parse_default_endianess(&attr_args);
    let trait_methods = input_trait.items.iter().filter_map(|item| {
        if let syn::TraitItem::Method(method) = item {
            Some(method)
        } else {
            None
        }
    });

    let mut method_impls = Vec::new();

    for method in trait_methods {
        // method_name: i32_le u24_be ...
        let method_name = &method.sig.ident;

        let (sign, bits, endianess) = parse_method_name(method_name, default_endianess);
        assert!(bits % 8 == 0, "invalid bits:{method_name}");

        // u8, i8, i32, u32, ...
        let ty = parse_return_type(&method.sig.output);
        let ty_name = ty.to_string();
        let ty_sign = ty_name.as_bytes()[0];
        let ty_bits = ty_name[1..].parse::<usize>().expect("{ty_name} not valid");

        assert_eq!(sign, ty_sign, "sign not match:{method_name} {ty_name}");
        assert!(ty_bits.is_power_of_two(), "invalid bits:{method_name}");
        assert!(ty_bits >= bits, "invalid bits:{method_name} {ty_name}");
        let method_args = &method.sig.inputs;
        assert!(validate_args(method_args), "invalid args:{method_name}");

        // 我们假设有一个参数oft: usize

        let from_endianess = match endianess {
            Endianess::Big => quote! { from_be_bytes },
            Endianess::Little => quote! { from_le_bytes },
        };
        let post = if bits < ty_bits {
            let shift = (ty_bits - bits) as usize;
            match endianess {
                // 大端字节序，右移
                Endianess::Big => quote! {
                    v >> #shift
                },
                // 小端要处理符号位
                Endianess::Little => quote! {
                    (v << #shift) as #ty >> #shift
                },
            }
        } else {
            quote! {v}
        };
        let copy_len = bits / 8;
        let size = ty_bits / 8;

        let method_impl = quote! {
            #[inline]
            fn #method_name(&self, oft: usize) -> #ty{
            debug_assert!(self.len() >= oft + #copy_len);
            let oft_start = self.mask(oft + self.start());
            let len = self.cap() - oft_start; // 从oft_start到cap的长度
            let v = if len >= #size {
                let b = unsafe { std::slice::from_raw_parts(self.ptr().add(oft_start), #size) };
                #ty::#from_endianess(b[..#size].try_into().unwrap())
            } else {
                // 分段读取
                let mut b = [0u8; #size];
                use std::ptr::copy_nonoverlapping as copy;
                let len = len.min(#copy_len);
                unsafe { copy(self.ptr().add(oft_start), b.as_mut_ptr(), len) };
                unsafe { copy(self.ptr(), b.as_mut_ptr().add(len), #copy_len - len) };
                #ty::#from_endianess(b)
            };
            #post
            }
        };
        method_impls.push(method_impl);
    }

    let trait_name = &input_trait.ident;
    let expanded = quote! {
        #input_trait

        impl #trait_name for RingSlice {
            #(#method_impls)*
        }
    };

    TokenStream::from(expanded)
}

fn parse_default_endianess(args: &AttributeArgs) -> Endianess {
    // 确定默认字节序，这里我们假设只有一个参数，要么是`be`要么是`le`
    if let Some(NestedMeta::Meta(syn::Meta::NameValue(nv))) = args.first() {
        if nv.path.is_ident("default") {
            if let syn::Lit::Str(ref endianess) = nv.lit {
                return endianess.value().as_str().into();
            }
        }
    }
    Endianess::Big
}

// method_name: "i16_le":
// 返回 i 16 le
fn parse_method_name(method: &Ident, default_endianess: Endianess) -> (u8, usize, Endianess) {
    let name_str = method.to_string();
    let sign = name_str.as_bytes()[0];
    let mut bits_endianess = name_str[1..].split('_');
    let bits = bits_endianess
        .next()
        .expect("invalid method name")
        .parse()
        .expect("method parse bits");
    let endianess = bits_endianess
        .next()
        .map(|s| s.into())
        .unwrap_or(default_endianess);
    (sign, bits, endianess)
}

fn validate_args(method_args: &Punctuated<FnArg, Comma>) -> bool {
    // 检查参数的数量是否为2
    if method_args.len() == 2 {
        // 检查第一个参数是否是`self`的引用
        if let Some(FnArg::Receiver(_)) = method_args.first() {
            // 检查第二个参数是否是`usize`
            if let Some(FnArg::Typed(PatType { ty, .. })) = method_args.last() {
                if let Type::Path(type_path) = &**ty {
                    if let Some(last_segment) = type_path.path.segments.last() {
                        // 确认类型是否为`usize`
                        return last_segment.ident == "usize";
                    }
                }
            }
        }
    }
    false
}

fn parse_return_type(return_type: &ReturnType) -> &Ident {
    match return_type {
        ReturnType::Type(_, ty) => {
            if let Type::Path(path) = &**ty {
                &path.path.segments.first().unwrap().ident
            } else {
                panic!("invalid return type")
            }
        }
        ReturnType::Default => panic!("missing return type"),
    }
}
