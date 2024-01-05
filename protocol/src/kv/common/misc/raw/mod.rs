// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! Various parsing/serialization primitives.

use std::io;

use ::bytes::BufMut;
use ds::RingSlice;
use smallvec::{Array, SmallVec};

use crate::kv::common::{
    io::ParseBuf,
    proto::{MyDeserialize, MySerialize},
};

use self::bytes::LenEnc;
pub use self::{
    _const::{Const, RawConst},
    bytes::RawBytes,
    flags::RawFlags,
    int::RawInt,
    seq::RawSeq,
};

use super::unexpected_buf_eof;

pub mod _const;
pub mod bytes;
pub mod flags;
pub mod int;
pub mod seq;

// 改造为基于RingSlice的反序列化
// impl<'de> MyDeserialize<'de> for &'de [u8] {
//     const SIZE: Option<usize> = None;
//     type Ctx = usize;

//     // fn deserialize(len: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
//     fn deserialize(len: Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
//         buf.checked_eat(len).ok_or_else(unexpected_buf_eof)
//     }
// }

// 注意check一致性 fishermen
impl MyDeserialize for RingSlice {
    const SIZE: Option<usize> = None;
    type Ctx = usize;

    // fn deserialize(len: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize(len: Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        buf.checked_eat(len).ok_or_else(unexpected_buf_eof)
    }
}

impl MySerialize for [u8] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_slice(self);
    }
}

impl MySerialize for RingSlice {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.copy_to_vec(buf);
    }
}

impl<const LEN: usize> MyDeserialize for [u8; LEN] {
    const SIZE: Option<usize> = Some(LEN);
    type Ctx = ();

    // fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        let value = buf.eat(LEN);
        let mut this = [0_u8; LEN];
        // this.copy_from_slice(value);

        value.copy_to_slice(&mut this);

        Ok(this)
    }
}

impl<const LEN: usize> MySerialize for [u8; LEN] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_slice(&self[..]);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Skip<const LEN: usize>;

impl<const LEN: usize> MyDeserialize for Skip<LEN> {
    const SIZE: Option<usize> = Some(LEN);
    type Ctx = ();

    // fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        buf.skip(LEN);
        Ok(Self)
    }
}

impl<const LEN: usize> MySerialize for Skip<LEN> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_slice(&[0_u8; LEN]);
    }
}

// impl<'de> MyDeserialize<'de> for ParseBuf<'de> {
impl MyDeserialize for ParseBuf {
    const SIZE: Option<usize> = None;
    type Ctx = usize;

    // fn deserialize(len: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize(len: Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        buf.checked_eat_buf(len).ok_or_else(unexpected_buf_eof)
    }
}

/// This ad-hock impl parses length-encoded string into a `SmallVec`.
impl<const LEN: usize> MyDeserialize for SmallVec<[u8; LEN]>
where
    [u8; LEN]: Array<Item = u8>,
{
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        let mut small_vec = SmallVec::new();
        let s: RawBytes<LenEnc> = buf.parse(())?;
        // small_vec.extend_from_slice(s.as_bytes());
        // Ok(small_vec)

        // SmallVec 目前只有在这里使用，后续有更多，就抽一个方法复用
        let (l, r) = s.as_bytes().data();
        small_vec.extend_from_slice(l);
        if r.len() > 0 {
            small_vec.extend_from_slice(r);
        }
        Ok(small_vec)
    }
}

impl<const LEN: usize> MySerialize for SmallVec<[u8; LEN]>
where
    [u8; LEN]: Array<Item = u8>,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_slice(&*self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Either<T, U> {
    Left(T),
    Right(U),
}

impl<T, U> MyDeserialize for Either<T, U>
where
    T: MyDeserialize,
    U: MyDeserialize,
{
    const SIZE: Option<usize> = None; // TODO: maybe later
    /// Which one to deserialize.
    type Ctx = Either<T::Ctx, U::Ctx>;

    // fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        match ctx {
            Either::Left(ctx) => T::deserialize(ctx, buf).map(Either::Left),
            Either::Right(ctx) => U::deserialize(ctx, buf).map(Either::Right),
        }
    }
}

impl<T, U> MySerialize for Either<T, U>
where
    T: MySerialize,
    U: MySerialize,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Either::Left(x) => x.serialize(buf),
            Either::Right(x) => x.serialize(buf),
        }
    }
}

impl MyDeserialize for f64 {
    const SIZE: Option<usize> = Some(8);
    type Ctx = ();

    // fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        Ok(buf.eat_f64_le())
    }
}

impl MySerialize for f64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_f64_le(*self);
    }
}
