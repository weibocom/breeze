use protocol::kv::common::value::convert::duration::MyDuration;
use protocol::kv::common::value::convert::regex::parse_mysql_time_string;
use protocol::kv::common::value::convert::{from_value, from_value_opt, FromValue};
use protocol::kv::common::value::Value;

use proptest::proptest;

mod value;

#[test]
fn test_my_duration() {
    let d: MyDuration = "123:45:56.789012".as_bytes().try_into().unwrap();
    assert_eq!(d, (123u32, 45u32, 56u32, 789012u32));

    let d: MyDuration = "123:45:56".as_bytes().try_into().unwrap();
    assert_eq!(d, (123, 45, 56, 0));
}

macro_rules! signed_primitive_roundtrip {
    ($t:ty, $name:ident) => {
        proptest! {
            #[test]
            fn $name(n: $t) {
                let val = Value::Int(n as i64);
                let val_bytes = Value::Bytes(n.to_string().into());
                assert_eq!(Value::from(from_value::<$t>(val.clone())), val);
                assert_eq!(Value::from(from_value::<$t>(val_bytes.clone())), val);
                if n >= 0 {
                    let val_uint = Value::UInt(n as u64);
                    assert_eq!(Value::from(from_value::<$t>(val_uint.clone())), val);
                }
            }
        }
    };
}

macro_rules! unsigned_primitive_roundtrip {
    ($t:ty, $name:ident) => {
        proptest! {
            #[test]
            fn $name(n: $t) {
                let val = Value::UInt(n as u64);
                let val_bytes = Value::Bytes(n.to_string().into());
                assert_eq!(Value::from(from_value::<$t>(val.clone())), val);
                assert_eq!(Value::from(from_value::<$t>(val_bytes.clone())), val);
                if n as u64 <= i64::max_value() as u64 {
                    let val_int = Value::Int(n as i64);
                    assert_eq!(Value::from(from_value::<$t>(val_int.clone())), val);
                }
            }
        }
    };
}

proptest! {
    #[test]
    fn bytes_roundtrip(s: Vec<u8>) {
        let val = Value::Bytes(s);
        assert_eq!(Value::from(from_value::<Vec<u8>>(val.clone())), val);
    }

    #[test]
    fn string_roundtrip(s: String) {
        let val = Value::Bytes(s.as_bytes().to_vec());
        assert_eq!(Value::from(from_value::<String>(val.clone())), val);
    }

    #[test]
    fn parse_mysql_time_string_parses_valid_time(
        s in r"-?[0-8][0-9][0-9]:[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?"
    ) {
        parse_mysql_time_string(s.as_bytes()).unwrap();

        if s.as_bytes()[0] != b'-'{
        let d:Result<MyDuration, String> = s.as_bytes().try_into();
            assert!(d.is_ok(), "{:?}", s);
        }

        // Don't test `parse_mysql_time_string_with_time` here,
        // as this tests valid MySQL TIME values, not valid time ranges within a day.
        // Due to that, `time::parse` will return an Err for invalid time strings.
    }

    #[test]
    fn parse_mysql_time_string_parses_correctly(
        sign in 0..2,
        h in 0u32..900,
        m in 0u32..59,
        s in 0u32..59,
        have_us in 0..2,
        us in 0u32..1000000,
    ) {
        let time_string = format!(
            "{}{:02}:{:02}:{:02}{}",
            if sign == 1 { "-" } else { "" },
            h, m, s,
            if have_us == 1 {
                format!(".{:06}", us)
            } else {
                "".into()
            }
        );
        let time = parse_mysql_time_string(time_string.as_bytes()).unwrap();
        assert_eq!(time, (sign == 1, h, m, s, if have_us == 1 { us } else { 0 }));

        if sign == 0 {
            let d:MyDuration = time_string.as_bytes().try_into().unwrap();
            assert_eq!(d, (h, m, s, if have_us == 1 { us } else { 0 }));
        }

        // Don't test `parse_mysql_time_string_with_time` here,
        // as this tests valid MySQL TIME values, not valid time ranges within a day.
        // Due to that, `time::parse` will return an Err for invalid time strings.
    }

    #[test]
    #[cfg(all(feature = "time", test))]
    fn parse_mysql_datetime_string_parses_valid_time(
        s in r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?"
    ) {
        parse_mysql_datetime_string(s.as_bytes()).unwrap();
    }

    #[test]
    #[cfg(all(feature = "time", test))]
    fn parse_mysql_datetime_string_doesnt_crash(s in "\\PC*") {
        parse_mysql_datetime_string(s.as_bytes());
        let _ = super::time::parse_mysql_datetime_string_with_time(s.as_bytes());
    }

    #[test]
    fn i128_roundtrip(
        bytes_pos in r"16[0-9]{37}",
        bytes_neg in r"-16[0-9]{37}",
        uint in (i64::max_value() as u64 + 1)..u64::max_value(),
        int: i64,
    ) {
        let val_bytes_pos = Value::Bytes(bytes_pos.as_bytes().into());
        let val_bytes_neg = Value::Bytes(bytes_neg.as_bytes().into());
        let val_uint = Value::UInt(uint);
        let val_int = Value::Int(int);

        assert_eq!(Value::from(from_value::<i128>(val_bytes_pos.clone())), val_bytes_pos);
        assert_eq!(Value::from(from_value::<i128>(val_bytes_neg.clone())), val_bytes_neg);
        assert_eq!(Value::from(from_value::<i128>(val_uint.clone())), val_uint);
        assert_eq!(Value::from(from_value::<i128>(val_int.clone())), val_int);
    }

    #[test]
    fn u128_roundtrip(
        bytes in r"16[0-9]{37}",
        uint: u64,
        int in 0i64..i64::max_value(),
    ) {
        let val_bytes = Value::Bytes(bytes.as_bytes().into());
        let val_uint = Value::UInt(uint);
        let val_int = Value::Int(int);

        assert_eq!(Value::from(from_value::<u128>(val_bytes.clone())), val_bytes);
        assert_eq!(Value::from(from_value::<u128>(val_uint.clone())), val_uint);
        assert_eq!(Value::from(from_value::<u128>(val_int)), Value::UInt(int as u64));
    }

    #[test]
    fn f32_roundtrip(n: f32) {
        let val = Value::Float(n);
        let val_bytes = Value::Bytes(n.to_string().into());
        assert_eq!(Value::from(from_value::<f32>(val.clone())), val);
        assert_eq!(Value::from(from_value::<f32>(val_bytes)), val);
    }

    #[test]
    fn f64_roundtrip(n: f64) {
        let val = Value::Double(n);
        let val_bytes = Value::Bytes(n.to_string().into());
        assert_eq!(Value::from(from_value::<f64>(val.clone())), val);
        assert_eq!(Value::from(from_value::<f64>(val_bytes)), val);
    }
}

signed_primitive_roundtrip!(i8, i8_roundtrip);
signed_primitive_roundtrip!(i16, i16_roundtrip);
signed_primitive_roundtrip!(i32, i32_roundtrip);
signed_primitive_roundtrip!(i64, i64_roundtrip);

unsigned_primitive_roundtrip!(u8, u8_roundtrip);
unsigned_primitive_roundtrip!(u16, u16_roundtrip);
unsigned_primitive_roundtrip!(u32, u32_roundtrip);
unsigned_primitive_roundtrip!(u64, u64_roundtrip);

#[test]
fn from_value_should_fail_on_integer_overflow() {
    let value = Value::Bytes(b"340282366920938463463374607431768211456"[..].into());
    assert!(from_value_opt::<u8>(value.clone()).is_err());
    assert!(from_value_opt::<i8>(value.clone()).is_err());
    assert!(from_value_opt::<u16>(value.clone()).is_err());
    assert!(from_value_opt::<i16>(value.clone()).is_err());
    assert!(from_value_opt::<u32>(value.clone()).is_err());
    assert!(from_value_opt::<i32>(value.clone()).is_err());
    assert!(from_value_opt::<u64>(value.clone()).is_err());
    assert!(from_value_opt::<i64>(value.clone()).is_err());
    assert!(from_value_opt::<u128>(value.clone()).is_err());
    assert!(from_value_opt::<i128>(value).is_err());
}

#[test]
fn from_value_should_fail_on_integer_underflow() {
    let value = Value::Bytes(b"-170141183460469231731687303715884105729"[..].into());
    assert!(from_value_opt::<u8>(value.clone()).is_err());
    assert!(from_value_opt::<i8>(value.clone()).is_err());
    assert!(from_value_opt::<u16>(value.clone()).is_err());
    assert!(from_value_opt::<i16>(value.clone()).is_err());
    assert!(from_value_opt::<u32>(value.clone()).is_err());
    assert!(from_value_opt::<i32>(value.clone()).is_err());
    assert!(from_value_opt::<u64>(value.clone()).is_err());
    assert!(from_value_opt::<i64>(value.clone()).is_err());
    assert!(from_value_opt::<u128>(value.clone()).is_err());
    assert!(from_value_opt::<i128>(value).is_err());
}

#[test]
fn value_float_read_conversions_work() {
    let original_f32 = std::f32::consts::PI;
    let float_value = Value::Float(original_f32);

    // Reading an f32 from a MySQL float works.
    let converted_f32: f32 = f32::from_value_opt(float_value.clone()).unwrap();
    assert_eq!(converted_f32, original_f32);

    // Reading an f64 from a MySQL float also works (lossless cast).
    let converted_f64: f64 = f64::from_value_opt(float_value).unwrap();
    assert_eq!(converted_f64, original_f32 as f64);
}

#[test]
fn value_double_read_conversions_work() {
    let original_f64 = std::f64::consts::PI;
    let double_value = Value::Double(original_f64);

    // Reading an f64 from a MySQL double works.
    let converted_f64: f64 = f64::from_value_opt(double_value.clone()).unwrap();
    assert_eq!(converted_f64, original_f64);

    // Reading an f32 from a MySQL double fails (precision loss).
    assert!(f32::from_value_opt(double_value).is_err());
}
