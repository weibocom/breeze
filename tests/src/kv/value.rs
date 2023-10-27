use std::io;

use protocol::kv::common::{value::Value, ParseBuf};

#[test]
fn should_escape_string() {
    assert_eq!(r"'?p??\\\\?p??'", Value::from("?p??\\\\?p??").as_sql(false));
    assert_eq!(r#"'?p??\"?p??'"#, Value::from("?p??\"?p??").as_sql(false));
    assert_eq!(r"'?p??\'?p??'", Value::from("?p??'?p??").as_sql(false));
    assert_eq!(r"'?p??\n?p??'", Value::from("?p??\n?p??").as_sql(false));
    assert_eq!(r"'?p??\r?p??'", Value::from("?p??\r?p??").as_sql(false));
    assert_eq!(r"'?p??\0?p??'", Value::from("?p??\x00?p??").as_sql(false));
}

#[cfg(feature = "nightly")]
mod benches {
    use std::convert::TryFrom;

    use crate::{
        constants::ColumnType,
        io::WriteMysqlExt,
        packets::{Column, ComStmtExecuteRequestBuilder, NullBitmap},
        value::{ClientSide, Value},
    };

    #[bench]
    fn bench_build_stmt_execute_request(bencher: &mut test::Bencher) {
        let values = vec![
            Value::Bytes(b"12.3456789".to_vec()),
            Value::Int(0xF0),
            Value::Int(0xF000),
            Value::Int(0xF0000000),
            Value::Float(std::f32::MAX),
            Value::Double(std::f64::MAX),
            Value::NULL,
            Value::Date(2019, 11, 27, 12, 30, 0, 123456),
            Value::UInt(0xF000000000000000),
            Value::Int(0xF00000),
            Value::Date(2019, 11, 27, 0, 0, 0, 0),
            Value::Time(true, 300, 8, 8, 8, 123456),
            Value::Date(2019, 11, 27, 12, 30, 0, 123456),
            Value::Int(2019),
            Value::Bytes(b"varchar".to_vec()),
            Value::Bytes(b"1000000110000001".to_vec()),
            Value::Bytes(br#"{"foo":"bar","baz":42345.6777}"#.to_vec()),
            Value::Bytes(b"12.3456789".to_vec()),
            Value::Bytes(b"Variant".to_vec()),
            Value::Bytes(b"Element".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_TINY_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_MEDIUM_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_LONG_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_VAR_STRING".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_STRING".to_vec()),
            Value::NULL,
            Value::Bytes(b"MYSQL_TYPE_GEOMETRY".to_vec()),
        ];

        let (body, _) = ComStmtExecuteRequestBuilder::new(0).build(&*values);

        bencher.bytes = body.len() as u64;
        bencher.iter(|| ComStmtExecuteRequestBuilder::new(0).build(&*values));
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn bench_parse_bin_row(bencher: &mut test::Bencher) {
        fn col(name: &str, ty: ColumnType) -> Column<'static> {
            let mut payload = b"\x00def".to_vec();
            for _ in 0..5 {
                payload.write_lenenc_str(name.as_bytes()).unwrap();
            }
            payload.extend_from_slice(&b"_\x2d\x00\xff\xff\xff\xff"[..]);
            payload.push(ty as u8);
            payload.extend_from_slice(&b"\x00\x00\x00"[..]);
            Column::read(&payload[..]).unwrap()
        }

        let values = vec![
            Value::Bytes(b"12.3456789".to_vec()),
            Value::Int(0xF0),
            Value::Int(0xF000),
            Value::Int(0xF0000000),
            Value::Float(std::f32::MAX),
            Value::Double(std::f64::MAX),
            Value::NULL,
            Value::Date(2019, 11, 27, 12, 30, 0, 123456),
            Value::UInt(0xF000000000000000),
            Value::Int(0xF00000),
            Value::Date(2019, 11, 27, 0, 0, 0, 0),
            Value::Time(true, 300, 8, 8, 8, 123456),
            Value::Date(2019, 11, 27, 12, 30, 0, 123456),
            Value::Int(2019),
            Value::Bytes(b"varchar".to_vec()),
            Value::Bytes(b"1000000110000001".to_vec()),
            Value::Bytes(br#"{"foo":"bar","baz":42345.6777}"#.to_vec()),
            Value::Bytes(b"12.3456789".to_vec()),
            Value::Bytes(b"Variant".to_vec()),
            Value::Bytes(b"Element".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_TINY_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_MEDIUM_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_LONG_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_BLOB".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_VAR_STRING".to_vec()),
            Value::Bytes(b"MYSQL_TYPE_STRING".to_vec()),
            Value::NULL,
            Value::Bytes(b"MYSQL_TYPE_GEOMETRY".to_vec()),
        ];

        let (body, _) = ComStmtExecuteRequestBuilder::new(0).build(&*values);

        let bitmap_len = NullBitmap::<ClientSide>::bitmap_len(values.len());

        let meta_offset = ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET + bitmap_len + 1;
        let meta_len = values.len() * 2;
        let columns = body[meta_offset..(meta_offset + meta_len)]
            .chunks(2)
            .map(|meta| col("foo", ColumnType::try_from(meta[0]).unwrap()))
            .collect::<Vec<_>>();

        let mut data = vec![0x00];
        data.extend_from_slice(
            &body[ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET
                ..(ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET + bitmap_len)],
        );
        data.extend_from_slice(
            &body[(ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET
                + bitmap_len
                + 1
                + 2 * values.len())..],
        );

        bencher.bytes = data.len() as u64;
        bencher.iter(|| Value::read_bin_many::<ClientSide>(&*data, &*columns).unwrap());
    }
}

#[test]
fn mysql_simple_issue_284() -> io::Result<()> {
    use ds::RingSlice;
    use Value::*;

    let data = vec![1, 49, 1, 50, 1, 51, 251, 1, 52, 1, 53, 251, 1, 55];
    let slice = RingSlice::from_vec(&data);
    let mut buf = ParseBuf::from(slice);
    assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"1".to_vec()));
    assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"2".to_vec()));
    assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"3".to_vec()));
    assert_eq!(Value::deserialize_text(&mut buf)?, NULL);
    assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"4".to_vec()));
    assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"5".to_vec()));
    assert_eq!(Value::deserialize_text(&mut buf)?, NULL);
    assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"7".to_vec()));

    Ok(())
}
