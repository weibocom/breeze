// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    collections::{hash_map::Entry, HashMap},
    error::Error,
    fmt,
};

use super::value::{convert::ToValue, Value};

/// `FromValue` conversion error.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MissingNamedParameterError(pub Vec<u8>);

impl fmt::Display for MissingNamedParameterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Missing named parameter `{}` for statement",
            String::from_utf8_lossy(&self.0)
        )
    }
}

impl Error for MissingNamedParameterError {
    fn description(&self) -> &str {
        "Missing named parameter for statement"
    }
}

/// Representations of parameters of a prepared statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Params {
    Empty,
    Named(HashMap<Vec<u8>, Value>),
    Positional(Vec<Value>),
}

impl<'a, T: Into<Params> + Clone> From<&'a T> for Params {
    fn from(x: &'a T) -> Params {
        x.clone().into()
    }
}

impl<T: Into<Value>> From<Vec<T>> for Params {
    fn from(x: Vec<T>) -> Params {
        let mut raw_params: Vec<Value> = Vec::with_capacity(x.len());
        for v in x {
            raw_params.push(v.into());
        }
        if raw_params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(raw_params)
        }
    }
}

impl<N, V> From<Vec<(N, V)>> for Params
where
    N: Into<Vec<u8>>,
    V: Into<Value>,
{
    fn from(x: Vec<(N, V)>) -> Params {
        let mut map = HashMap::default();
        for (name, value) in x {
            let name: Vec<u8> = name.into();
            match map.entry(name) {
                Entry::Vacant(entry) => entry.insert(value.into()),
                Entry::Occupied(entry) => {
                    panic!(
                        "Redefinition of named parameter `{}'",
                        String::from_utf8_lossy(entry.key())
                    );
                }
            };
        }
        Params::Named(map)
    }
}

impl<'a> From<&'a [&'a dyn ToValue]> for Params {
    fn from(x: &'a [&'a dyn ToValue]) -> Params {
        let mut raw_params: Vec<Value> = Vec::new();
        for v in x {
            raw_params.push(v.to_value());
        }
        if raw_params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(raw_params)
        }
    }
}

impl From<()> for Params {
    fn from(_: ()) -> Params {
        Params::Empty
    }
}

use seq_macro::seq;
// This macro generates `From<(T0, T1, ...)> for Params` impls for tuples of size 1..=12.
macro_rules! tuple_into_params {
    ($count:literal) => {
        seq!(N in 0..$count {
            impl<#(T~N:Into<Value>,)*> From<(#(T~N,)*)> for Params {
                fn from(x: (#(T~N,)*)) -> Params {
                    Params::Positional(vec![
                        #(x.N.into(),)*
                    ])
                }
            }
        });
    }
}

seq!(N in 1..=12 {
    tuple_into_params!(N);
});
