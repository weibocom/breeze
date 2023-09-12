// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use lexical::parse;
use num_traits::{FromPrimitive, ToPrimitive};

use std::{any::type_name, error::Error, fmt, str::from_utf8, time::Duration};

use crate::kv::common::value::Value;
use duration::parse_duration;

macro_rules! impl_from_value {
    ($ty:ty, $ir:ty) => {
        impl crate::kv::common::value::convert::FromValue for $ty {
            type Intermediate = $ir;
        }
    };
}

pub mod bigdecimal;
pub mod bigdecimal03;
pub mod bigint;
pub mod chrono;
pub mod decimal;
pub mod time;
pub mod time03;
pub mod uuid;

pub mod regex;

pub mod duration;

/// `FromValue` conversion error.
#[derive(Debug, Clone, PartialEq)]
pub struct FromValueError(pub Value);

impl fmt::Display for FromValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Couldn't convert the value `{:?}` to a desired type",
            self.0
        )
    }
}

impl Error for FromValueError {
    fn description(&self) -> &str {
        "Couldn't convert the value to a desired type"
    }
}

/// Basic operations on `FromValue` conversion intermediate result.
///
/// See [`FromValue`](trait.FromValue.html)
pub trait ConvIr<T>: Sized {
    fn new(v: Value) -> Result<Self, FromValueError>;
    fn commit(self) -> T;
    fn rollback(self) -> Value;
}

/// Implement this trait to convert value to something.
///
/// `FromRow` requires ability to cheaply rollback `FromValue` conversion. This ability is
/// provided via `Intermediate` associated type.
///
/// Example implementation:
///
/// ```ignore
/// #[derive(Debug)]
/// pub struct StringIr {
///     bytes: Vec<u8>,
/// }
///
/// impl ConvIr<String> for StringIr {
///     fn new(v: Value) -> MyResult<StringIr> {
///         match v {
///             Value::Bytes(bytes) => match from_utf8(&*bytes) {
///                 Ok(_) => Ok(StringIr { bytes: bytes }),
///                 Err(_) => Err(Error::FromValueError(Value::Bytes(bytes))),
///             },
///             v => Err(Error::FromValueError(v)),
///         }
///     }
///     fn commit(self) -> String {
///         unsafe { String::from_utf8_unchecked(self.bytes) }
///     }
///     fn rollback(self) -> Value {
///         Value::Bytes(self.bytes)
///     }
/// }
///
/// impl FromValue for String {
///     type Intermediate = StringIr;
/// }
/// ```
pub trait FromValue: Sized {
    type Intermediate: ConvIr<Self>;

    /// Will panic if could not convert `v` to `Self`.
    fn from_value(v: Value) -> Self {
        match Self::from_value_opt(v) {
            Ok(this) => this,
            Err(_) => panic!("Could not retrieve {} from Value", type_name::<Self>()),
        }
    }

    /// Will return `Err(Error::FromValueError(v))` if could not convert `v` to `Self`.
    fn from_value_opt(v: Value) -> Result<Self, FromValueError> {
        let ir = Self::Intermediate::new(v)?;
        Ok(ir.commit())
    }

    /// Will return `Err(Error::FromValueError(v))` if `v` is not convertible to `Self`.
    fn get_intermediate(v: Value) -> Result<Self::Intermediate, FromValueError> {
        Self::Intermediate::new(v)
    }
}

/// Will panic if could not convert `v` to `T`
pub fn from_value<T: FromValue>(v: Value) -> T {
    FromValue::from_value(v)
}

/// Will return `Err(FromValueError(v))` if could not convert `v` to `T`
pub fn from_value_opt<T: FromValue>(v: Value) -> Result<T, FromValueError> {
    FromValue::from_value_opt(v)
}

macro_rules! impl_from_value_num {
    ($t:ident) => {
        impl ConvIr<$t> for ParseIr<$t> {
            fn new(v: Value) -> Result<ParseIr<$t>, FromValueError> {
                match v {
                    Value::Int(x) => {
                        if let Some(output) = $t::from_i64(x) {
                            Ok(ParseIr {
                                value: Value::Int(x),
                                output,
                            })
                        } else {
                            Err(FromValueError(Value::Int(x)))
                        }
                    }
                    Value::UInt(x) => {
                        if let Some(output) = $t::from_u64(x) {
                            Ok(ParseIr {
                                value: Value::UInt(x),
                                output,
                            })
                        } else {
                            Err(FromValueError(Value::UInt(x)))
                        }
                    }
                    Value::Bytes(bytes) => match parse(&*bytes) {
                        Ok(x) => Ok(ParseIr {
                            value: Value::Bytes(bytes),
                            output: x,
                        }),
                        _ => Err(FromValueError(Value::Bytes(bytes))),
                    },
                    v => Err(FromValueError(v)),
                }
            }
            fn commit(self) -> $t {
                self.output
            }
            fn rollback(self) -> Value {
                self.value
            }
        }

        impl_from_value!($t, ParseIr<$t>);
    };
}

/// Intermediate result of a Value-to-Option<T> conversion.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionIr<T> {
    value: Option<Value>,
    ir: Option<T>,
}

impl<T, Ir> ConvIr<Option<T>> for OptionIr<Ir>
where
    T: FromValue<Intermediate = Ir>,
    Ir: ConvIr<T>,
{
    fn new(v: Value) -> Result<OptionIr<Ir>, FromValueError> {
        match v {
            Value::NULL => Ok(OptionIr {
                value: Some(Value::NULL),
                ir: None,
            }),
            v => match T::get_intermediate(v) {
                Ok(ir) => Ok(OptionIr {
                    value: None,
                    ir: Some(ir),
                }),
                Err(err) => Err(err),
            },
        }
    }
    fn commit(self) -> Option<T> {
        self.ir.map(|ir| ir.commit())
    }
    fn rollback(self) -> Value {
        let OptionIr { value, ir } = self;
        match value {
            Some(v) => v,
            None => match ir {
                Some(ir) => ir.rollback(),
                None => unreachable!(),
            },
        }
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    type Intermediate = OptionIr<T::Intermediate>;
}

impl ConvIr<Value> for Value {
    fn new(v: Value) -> Result<Self, FromValueError> {
        Ok(v)
    }

    fn commit(self) -> Value {
        self
    }

    fn rollback(self) -> Value {
        self
    }
}

impl FromValue for Value {
    type Intermediate = Value;
    fn from_value(v: Value) -> Value {
        v
    }
    fn from_value_opt(v: Value) -> Result<Value, FromValueError> {
        Ok(v)
    }
}

impl ConvIr<String> for Vec<u8> {
    fn new(v: Value) -> Result<Vec<u8>, FromValueError> {
        match v {
            Value::Bytes(bytes) => match from_utf8(&*bytes) {
                Ok(_) => Ok(bytes),
                Err(_) => Err(FromValueError(Value::Bytes(bytes))),
            },
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> String {
        unsafe { String::from_utf8_unchecked(self) }
    }
    fn rollback(self) -> Value {
        Value::Bytes(self)
    }
}

/// Intermediate result of a Value-to-Integer conversion.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseIr<T> {
    value: Value,
    output: T,
}

impl ConvIr<i64> for ParseIr<i64> {
    fn new(v: Value) -> Result<ParseIr<i64>, FromValueError> {
        match v {
            Value::Int(x) => Ok(ParseIr {
                value: Value::Int(x),
                output: x,
            }),
            Value::UInt(x) if x <= ::std::i64::MAX as u64 => Ok(ParseIr {
                value: Value::UInt(x),
                output: x as i64,
            }),
            Value::Bytes(bytes) => match parse(&*bytes) {
                Ok(x) => Ok(ParseIr {
                    value: Value::Bytes(bytes),
                    output: x,
                }),
                _ => Err(FromValueError(Value::Bytes(bytes))),
            },
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> i64 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<u64> for ParseIr<u64> {
    fn new(v: Value) -> Result<ParseIr<u64>, FromValueError> {
        match v {
            Value::Int(x) if x >= 0 => Ok(ParseIr {
                value: Value::Int(x),
                output: x as u64,
            }),
            Value::UInt(x) => Ok(ParseIr {
                value: Value::UInt(x),
                output: x,
            }),
            Value::Bytes(bytes) => match parse(&*bytes) {
                Ok(x) => Ok(ParseIr {
                    value: Value::Bytes(bytes),
                    output: x,
                }),
                _ => Err(FromValueError(Value::Bytes(bytes))),
            },
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> u64 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<f32> for ParseIr<f32> {
    fn new(v: Value) -> Result<ParseIr<f32>, FromValueError> {
        match v {
            Value::Float(x) => Ok(ParseIr {
                value: Value::Float(x),
                output: x,
            }),
            Value::Bytes(bytes) => {
                let val = parse(&*bytes).ok();
                match val {
                    Some(x) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: x,
                    }),
                    None => Err(FromValueError(Value::Bytes(bytes))),
                }
            }
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> f32 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<f64> for ParseIr<f64> {
    fn new(v: Value) -> Result<ParseIr<f64>, FromValueError> {
        match v {
            Value::Double(x) => Ok(ParseIr {
                value: Value::Double(x),
                output: x,
            }),
            Value::Float(x) => {
                let double = x.into();
                Ok(ParseIr {
                    value: Value::Double(double),
                    output: double,
                })
            }
            Value::Bytes(bytes) => {
                let val = parse(&*bytes).ok();
                match val {
                    Some(x) => Ok(ParseIr {
                        value: Value::Bytes(bytes),
                        output: x,
                    }),
                    _ => Err(FromValueError(Value::Bytes(bytes))),
                }
            }
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> f64 {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<bool> for ParseIr<bool> {
    fn new(v: Value) -> Result<ParseIr<bool>, FromValueError> {
        match v {
            Value::Int(0) => Ok(ParseIr {
                value: Value::Int(0),
                output: false,
            }),
            Value::Int(1) => Ok(ParseIr {
                value: Value::Int(1),
                output: true,
            }),
            Value::Bytes(bytes) => {
                if bytes.len() == 1 {
                    match bytes[0] {
                        0x30 => Ok(ParseIr {
                            value: Value::Bytes(bytes),
                            output: false,
                        }),
                        0x31 => Ok(ParseIr {
                            value: Value::Bytes(bytes),
                            output: true,
                        }),
                        _ => Err(FromValueError(Value::Bytes(bytes))),
                    }
                } else {
                    Err(FromValueError(Value::Bytes(bytes)))
                }
            }
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> bool {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl ConvIr<Vec<u8>> for Vec<u8> {
    fn new(v: Value) -> Result<Vec<u8>, FromValueError> {
        match v {
            Value::Bytes(bytes) => Ok(bytes),
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> Vec<u8> {
        self
    }
    fn rollback(self) -> Value {
        Value::Bytes(self)
    }
}

impl ConvIr<Duration> for ParseIr<Duration> {
    fn new(v: Value) -> Result<ParseIr<Duration>, FromValueError> {
        match v {
            Value::Time(false, days, hours, minutes, seconds, microseconds) => {
                let nanos = (microseconds as u32) * 1000;
                let secs = u64::from(seconds)
                    + u64::from(minutes) * 60
                    + u64::from(hours) * 60 * 60
                    + u64::from(days) * 60 * 60 * 24;
                Ok(ParseIr {
                    value: Value::Time(false, days, hours, minutes, seconds, microseconds),
                    output: Duration::new(secs, nanos),
                })
            }
            Value::Bytes(val_bytes) => parse_duration(&*val_bytes)
                .ok_or_else(|| FromValueError(Value::Bytes(val_bytes.clone())))
                .map(|d| ParseIr {
                    value: Value::Bytes(val_bytes),
                    output: d,
                }),
            v => Err(FromValueError(v)),
        }
    }
    fn commit(self) -> Duration {
        self.output
    }
    fn rollback(self) -> Value {
        self.value
    }
}

impl From<Duration> for Value {
    fn from(x: Duration) -> Value {
        let mut secs_total = x.as_secs();
        let micros = (f64::from(x.subsec_nanos()) / 1000_f64).round() as u32;
        let seconds = (secs_total % 60) as u8;
        secs_total -= u64::from(seconds);
        let minutes = ((secs_total % (60 * 60)) / 60) as u8;
        secs_total -= u64::from(minutes) * 60;
        let hours = ((secs_total % (60 * 60 * 24)) / (60 * 60)) as u8;
        secs_total -= u64::from(hours) * 60 * 60;
        Value::Time(
            false,
            (secs_total / (60 * 60 * 24)) as u32,
            hours,
            minutes,
            seconds,
            micros,
        )
    }
}

impl_from_value!(String, Vec<u8>);
impl_from_value!(Vec<u8>, Vec<u8>);
impl_from_value!(bool, ParseIr<bool>);
impl_from_value!(i64, ParseIr<i64>);
impl_from_value!(u64, ParseIr<u64>);
impl_from_value!(f32, ParseIr<f32>);
impl_from_value!(f64, ParseIr<f64>);
impl_from_value!(Duration, ParseIr<Duration>);
impl_from_value_num!(i8);
impl_from_value_num!(u8);
impl_from_value_num!(i16);
impl_from_value_num!(u16);
impl_from_value_num!(i32);
impl_from_value_num!(u32);
impl_from_value_num!(isize);
impl_from_value_num!(usize);
impl_from_value_num!(i128);
impl_from_value_num!(u128);

pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl<T: Into<Value> + Clone> ToValue for T {
    fn to_value(&self) -> Value {
        self.clone().into()
    }
}

impl<'a, T: ToValue> From<&'a T> for Value {
    fn from(x: &'a T) -> Value {
        x.to_value()
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(x: Option<T>) -> Value {
        match x {
            None => Value::NULL,
            Some(x) => x.into(),
        }
    }
}

macro_rules! into_value_impl (
    (signed $t:ty) => (
        impl From<$t> for Value {
            fn from(x: $t) -> Value {
                Value::Int(x as i64)
            }
        }
    );
    (unsigned $t:ty) => (
        impl From<$t> for Value {
            fn from(x: $t) -> Value {
                Value::UInt(x as u64)
            }
        }
    );
);

into_value_impl!(signed i8);
into_value_impl!(signed i16);
into_value_impl!(signed i32);
into_value_impl!(signed i64);
into_value_impl!(signed isize);
into_value_impl!(unsigned u8);
into_value_impl!(unsigned u16);
into_value_impl!(unsigned u32);
into_value_impl!(unsigned u64);
into_value_impl!(unsigned usize);

impl From<i128> for Value {
    fn from(x: i128) -> Value {
        if let Some(x) = x.to_i64() {
            Value::Int(x)
        } else if let Some(x) = x.to_u64() {
            Value::UInt(x)
        } else {
            Value::Bytes(x.to_string().into())
        }
    }
}

impl From<u128> for Value {
    fn from(x: u128) -> Value {
        if let Some(x) = x.to_u64() {
            Value::UInt(x)
        } else {
            Value::Bytes(x.to_string().into())
        }
    }
}

impl From<f32> for Value {
    fn from(x: f32) -> Value {
        Value::Float(x)
    }
}

impl From<f64> for Value {
    fn from(x: f64) -> Value {
        Value::Double(x)
    }
}

impl From<bool> for Value {
    fn from(x: bool) -> Value {
        Value::Int(if x { 1 } else { 0 })
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(x: &'a [u8]) -> Value {
        Value::Bytes(x.into())
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Value {
        Value::Bytes(x)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(x: &'a str) -> Value {
        let string: String = x.into();
        Value::Bytes(string.into_bytes())
    }
}

impl From<String> for Value {
    fn from(x: String) -> Value {
        Value::Bytes(x.into_bytes())
    }
}

macro_rules! from_array_impl {
    ($n:expr) => {
        impl From<[u8; $n]> for Value {
            fn from(x: [u8; $n]) -> Value {
                Value::from(&x[..])
            }
        }
    };
}

use seq_macro::seq;

seq!(N in 0..=32 {
    from_array_impl!(N);
});
