// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

// use std::borrow::Cow;

/// Appears if a statement have both named and positional parameters.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MixedParamsError;

// enum ParserState {
//     TopLevel,
//     // (string_delimiter, last_char)
//     InStringLiteral(u8, u8),
//     MaybeInNamedParam,
//     InNamedParam,
// }

// use self::ParserState::*;

// /// Returns pair of:
// ///
// /// * names of named parameters (if any) in order of appearance in `query`. Same name may
// ///   appear multiple times if named parameter used more than once.
// /// * query string to pass to MySql (named parameters replaced with `?`).
// pub fn parse_named_params(
//     query: &[u8],
// ) -> Result<(Option<Vec<Vec<u8>>>, Cow<'_, [u8]>), MixedParamsError> {
//     let mut state = TopLevel;
//     let mut have_positional = false;
//     let mut cur_param = 0;
//     // Vec<(start_offset, end_offset, name)>
//     let mut params = Vec::new();
//     for (i, c) in query.iter().enumerate() {
//         let mut rematch = false;
//         match state {
//             TopLevel => match c {
//                 b':' => state = MaybeInNamedParam,
//                 b'\'' => state = InStringLiteral(b'\'', b'\''),
//                 b'"' => state = InStringLiteral(b'"', b'"'),
//                 b'?' => have_positional = true,
//                 _ => (),
//             },
//             InStringLiteral(separator, prev_char) => match c {
//                 x if *x == separator && prev_char != b'\\' => state = TopLevel,
//                 x => state = InStringLiteral(separator, *x),
//             },
//             MaybeInNamedParam => match c {
//                 b'a'..=b'z' | b'_' => {
//                     params.push((i - 1, 0, Vec::with_capacity(16)));
//                     params[cur_param].2.push(*c);
//                     state = InNamedParam;
//                 }
//                 _ => rematch = true,
//             },
//             InNamedParam => match c {
//                 b'a'..=b'z' | b'0'..=b'9' | b'_' => params[cur_param].2.push(*c),
//                 _ => {
//                     params[cur_param].1 = i;
//                     cur_param += 1;
//                     rematch = true;
//                 }
//             },
//         }
//         if rematch {
//             match c {
//                 b':' => state = MaybeInNamedParam,
//                 b'\'' => state = InStringLiteral(b'\'', b'\''),
//                 b'"' => state = InStringLiteral(b'"', b'"'),
//                 _ => state = TopLevel,
//             }
//         }
//     }
//     if let InNamedParam = state {
//         params[cur_param].1 = query.len();
//     }
//     if !params.is_empty() {
//         if have_positional {
//             return Err(MixedParamsError);
//         }
//         let mut real_query = Vec::with_capacity(query.len());
//         let mut last = 0;
//         let mut out_params = Vec::with_capacity(params.len());
//         for (start, end, name) in params.into_iter() {
//             real_query.extend(&query[last..start]);
//             real_query.push(b'?');
//             last = end;
//             out_params.push(name);
//         }
//         real_query.extend(&query[last..]);
//         Ok((Some(out_params), real_query.into()))
//     } else {
//         Ok((None, query.into()))
//     }
// }

// #[cfg(test)]
// mod test {
//     use crate::kv::common::named_params::parse_named_params;

//     #[test]
//     fn should_parse_named_params() {
//         let result = parse_named_params(b":a :b").unwrap();
//         assert_eq!(
//             (
//                 Some(vec![b"a".to_vec(), b"b".to_vec()]),
//                 (&b"? ?"[..]).into()
//             ),
//             result
//         );

//         let result = parse_named_params(b"SELECT (:a-10)").unwrap();
//         assert_eq!(
//             (Some(vec![b"a".to_vec()]), (&b"SELECT (?-10)"[..]).into()),
//             result
//         );

//         let result = parse_named_params(br#"SELECT '"\':a' "'\"':c" :b"#).unwrap();
//         assert_eq!(
//             (
//                 Some(vec![b"b".to_vec()]),
//                 (&br#"SELECT '"\':a' "'\"':c" ?"#[..]).into(),
//             ),
//             result
//         );

//         let result = parse_named_params(br":a_Aa:b").unwrap();
//         assert_eq!(
//             (
//                 Some(vec![b"a_".to_vec(), b"b".to_vec()]),
//                 (&br"?Aa?"[..]).into()
//             ),
//             result
//         );

//         let result = parse_named_params(br"::b").unwrap();
//         assert_eq!((Some(vec![b"b".to_vec()]), (&br":?"[..]).into()), result);

//         parse_named_params(br":a ?").unwrap_err();
//     }

//     #[test]
//     fn should_allow_numbers_in_param_name() {
//         let result = parse_named_params(b":a1 :a2").unwrap();
//         assert_eq!(
//             (
//                 Some(vec![b"a1".to_vec(), b"a2".to_vec()]),
//                 (&b"? ?"[..]).into()
//             ),
//             result
//         );

//         let result = parse_named_params(b":1a :2a").unwrap();
//         assert_eq!((None, (&b":1a :2a"[..]).into()), result);
//     }

//     #[test]
//     fn special_characters_in_query() {
//         let result =
//             parse_named_params(r"SELECT 1 FROM été WHERE thing = :param;".as_bytes()).unwrap();
//         assert_eq!(
//             (
//                 Some(vec![b"param".to_vec()]),
//                 "SELECT 1 FROM été WHERE thing = ?;".as_bytes().into(),
//             ),
//             result
//         );
//     }

//     #[cfg(feature = "nightly")]
//     mod bench {
//         use crate::named_params::parse_named_params;

//         #[bench]
//         fn parse_ten_named_params(bencher: &mut test::Bencher) {
//             bencher.iter(|| {
//                 let result = parse_named_params(
//                     r#"
//                 SELECT :one, :two, :three, :four, :five, :six, :seven, :eight, :nine, :ten
//                 "#,
//                 )
//                 .unwrap();
//                 test::black_box(result);
//             });
//         }

//         #[bench]
//         fn parse_zero_named_params(bencher: &mut test::Bencher) {
//             bencher.iter(|| {
//                 let result = parse_named_params(
//                     r"
//                 SELECT one, two, three, four, five, six, seven, eight, nine, ten
//                 ",
//                 )
//                 .unwrap();
//                 test::black_box(result);
//             });
//         }
//     }
// }
