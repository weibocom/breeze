// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use super::int::LenEnc;

use std::{cmp::min, fmt, io, marker::PhantomData};

use bytes::BufMut;
use ds::RingSlice;

use crate::kv::common::{
    io::{BufMutExt, ParseBuf},
    misc::unexpected_buf_eof,
    proto::{MyDeserialize, MySerialize},
};

use super::{int::VarLen, RawInt};

/// Wrapper for a raw byte sequence, that came from a server.
///
/// `T` encodes the serialized representation.
#[derive(Clone, Default, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct RawBytes<T: BytesRepr>(pub RingSlice, PhantomData<T>);
// pub struct RawBytes<'a, T: BytesRepr>(pub Cow<'a, [u8]>, PhantomData<T>);

impl<T: BytesRepr> RawBytes<T> {
    /// Wraps the given value.
    // pub fn new(text: impl Into<Cow<'a, [u8]>>) -> Self {
    pub fn new(text: RingSlice) -> Self {
        // Self(text.into(), PhantomData)
        Self(text, PhantomData)
    }

    /// Converts self to a 'static version.
    pub fn into_owned(self) -> RawBytes<T> {
        RawBytes(self.0, PhantomData)
    }

    /// Returns `true` if bytes is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the _effective_ length of a string, which is no more than `T::MAX_LEN`.
    pub fn len(&self) -> usize {
        // min(self.0.as_ref().len(), T::MAX_LEN)
        min(self.0.len(), T::MAX_LEN)
    }

    // TODO 先返回折返slice，不轻易copy，方法名先不修改，等后面重构 fishermen
    // /// Returns the _effective_ bytes (see `RawBytes::len`).
    // pub fn as_bytes(&'a self) -> &'a [u8] {
    pub fn as_bytes(&self) -> &RingSlice {
        // &self.0.as_ref()[..self.len()]
        &self.0
    }

    // 先改为返回String，会存在copy，理论上最大长度很小，不会超过512
    /// Returns the value as a UTF-8 string (lossy contverted).
    // pub fn as_str(&'a self) -> Cow<'a, str> {
    pub fn as_str(&self) -> String {
        // String::from_utf8_lossy(self.as_bytes())

        debug_assert!(self.0.len() <= 512, "slice too big:{:?}", self.0);
        self.0.as_string_lossy()

        // &self.0
    }
}

impl<T: Into<RingSlice>, U: BytesRepr> From<T> for RawBytes<U> {
    fn from(bytes: T) -> RawBytes<U> {
        // RawBytes::new(bytes)
        RawBytes::new(bytes.into())
    }
}

impl<T: BytesRepr> PartialEq<[u8]> for RawBytes<T> {
    fn eq(&self, other: &[u8]) -> bool {
        // self.0.as_ref().eq(other)
        self.0.eq(other)
    }
}

impl<T: BytesRepr> MySerialize for RawBytes<T> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        // T::serialize(self.0.as_ref(), buf)
        T::serialize(&self.0, buf)
    }
}

impl<T: BytesRepr> MyDeserialize for RawBytes<T> {
    const SIZE: Option<usize> = T::SIZE;
    type Ctx = T::Ctx;

    #[inline(always)]
    // fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        // Ok(Self(T::deserialize(ctx, buf)?, PhantomData))
        let t = T::deserialize(ctx, buf)?;
        Ok(Self(t, PhantomData))
    }
}

impl<T: BytesRepr> fmt::Debug for RawBytes<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // let value_data = self.as_str();
        // let value = match value_data.try_oneway_slice(0, value_data.len()) {
        //     Some(d) => String::from_utf8_lossy(d),
        //     None => {
        //         let data = value_data.dump_ring_part(0, value_data.len());
        //         String::from_utf8_lossy(&data[..])
        //     }
        // };

        f.debug_struct("RawBytes")
            .field("value", &self.as_str())
            // .field("value", &value)
            .field(
                "max_len",
                &(if self.0.len() <= T::MAX_LEN {
                    format!("{}", T::MAX_LEN)
                } else {
                    format!("{} EXCEEDED!", T::MAX_LEN)
                }),
            )
            .finish()
    }
}

/// Representation of a serialized bytes.
pub trait BytesRepr {
    /// Maximum length of bytes for this repr (depends on how lenght is stored).
    const MAX_LEN: usize;
    const SIZE: Option<usize>;
    type Ctx;

    fn serialize(text: &RingSlice, buf: &mut Vec<u8>);

    /// Implementation must check the length of the buffer if `Self::SIZE.is_none()`.
    // fn deserialize<'de>(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>>;
    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice>;
}

impl BytesRepr for LenEnc {
    const MAX_LEN: usize = usize::MAX;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        buf.put_lenenc_int(text.len() as u64);
        // buf.put_slice(text);
        text.copy_to_vec(buf);
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        let len = buf.parse::<RawInt<LenEnc>>(())?;
        // buf.checked_eat(len.0 as usize)
        //     .map(Cow::Borrowed)
        //     .ok_or_else(unexpected_buf_eof)
        buf.checked_eat(len.0 as usize)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// A byte sequence prepended by it's u8 length.
///
/// `serialize` will truncate byte sequence if its too long.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct U8Bytes;

impl BytesRepr for U8Bytes {
    const MAX_LEN: usize = u8::MAX as usize;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        buf.put_u8_str(text);
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        let len: RawInt<u8> = buf.parse(())?;
        // buf.checked_eat(len.0 as usize)
        //     .map(Cow::Borrowed)
        //     .ok_or_else(unexpected_buf_eof)
        buf.checked_eat(len.0 as usize)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// A byte sequence prepended by it's u32 length.
///
/// `serialize` will truncate byte sequence if its too long.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct U32Bytes;

impl BytesRepr for U32Bytes {
    const MAX_LEN: usize = u32::MAX as usize;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        buf.put_u32_str(text);
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        // buf.checked_eat_u32_str()
        //     .map(Cow::Borrowed)
        //     .ok_or_else(unexpected_buf_eof)
        // 参考什么代码，注意check一致性 fishermen
        buf.checked_eat_u32_str().ok_or_else(unexpected_buf_eof)
    }
}

/// Null-terminated byte sequence.
///
/// `deserialize()` will error with `InvalidData` if there is no `0`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NullBytes;

impl BytesRepr for NullBytes {
    const MAX_LEN: usize = usize::MAX;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        // let last = text
        //     .iter()
        //     .position(|x| *x == 0)
        //     .unwrap_or_else(|| text.len());
        // buf.put_slice(&text[..last]);
        // buf.put_u8(0);
        // 参考上面的逻辑，check一致性，暂时不要清理 fishermen
        let last = match text.find(0, 0) {
            Some(p) => p,
            None => text.len(),
        };
        text.copy_to_vec_with_len(buf, last);
        buf.put_u8(0);
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        // match buf.0.iter().position(|x| *x == 0) {
        //     Some(i) => {
        //         let out = buf.eat(i);
        //         buf.skip(1);
        //         Ok(Cow::Borrowed(out))
        //     }
        //     None => Err(io::Error::new(
        //         io::ErrorKind::InvalidData,
        //         "no null terminator for null-terminated string",
        //     )),
        // }
        // 参考上面的逻辑，check一致性，暂时不要清理 fishermen
        match buf.find(0, 0) {
            Some(i) => {
                let out = buf.eat(i);
                buf.skip(1);
                Ok(out)
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no null terminator for null-terminated string",
            )),
        }
    }
}

/// A byte sequence that lasts from the current position to the end of the buffer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EofBytes;

impl BytesRepr for EofBytes {
    const MAX_LEN: usize = usize::MAX;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        // buf.put_slice(text);
        text.copy_to_vec(buf)
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        // Ok(Cow::Borrowed(buf.eat_all()))
        Ok(buf.eat_all())
    }
}

/// A byte sequence without length.
///
/// Its length is stored somewhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BareBytes<const MAX_LEN: usize>;

impl<const MAX_LEN: usize> BytesRepr for BareBytes<MAX_LEN> {
    const MAX_LEN: usize = MAX_LEN;
    const SIZE: Option<usize> = None;
    type Ctx = usize;

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        // let len = min(text.len(), MAX_LEN);
        // buf.put_slice(&text[..len]);
        // 参考上面的逻辑，check一致性，暂时不要清理 fishermen
        text.copy_to_vec_with_len(buf, MAX_LEN);
    }

    // fn deserialize<'de>(len: usize, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize(len: usize, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        // buf.checked_eat(len)
        //     .ok_or_else(unexpected_buf_eof)
        //     .map(Cow::Borrowed)
        buf.checked_eat(len).ok_or_else(unexpected_buf_eof)
    }
}

// /// `BareBytes` with `u8` len.
// pub type BareU8Bytes = BareBytes<{ u8::MAX as usize }>;

// /// `BareBytes` with `u16` len.
// pub type BareU16Bytes = BareBytes<{ u16::MAX as usize }>;

/// A fixed length byte sequence (right-padded with `0x00`).
///
/// `serialize()` truncates the value if it's loo long.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedLengthText<const LEN: usize>;

impl<const LEN: usize> BytesRepr for FixedLengthText<LEN> {
    const MAX_LEN: usize = LEN;
    const SIZE: Option<usize> = Some(LEN);
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        let len = min(LEN, text.len());
        // buf.put_slice(&text[..len]);
        text.copy_to_vec_with_len(buf, len);
        for _ in 0..(LEN - len) {
            buf.put_u8(0);
        }
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        match Self::SIZE {
            // Some(len) => Ok(Cow::Borrowed(buf.eat(len))),
            // None => buf
            //     .checked_eat(LEN)
            //     .map(Cow::Borrowed)
            //     .ok_or_else(unexpected_buf_eof),
            Some(len) => Ok(buf.eat(len)),
            None => buf.checked_eat(LEN).ok_or_else(unexpected_buf_eof),
        }
    }
}

impl BytesRepr for VarLen {
    const MAX_LEN: usize = u32::MAX as usize;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    // fn serialize(text: &[u8], buf: &mut Vec<u8>) {
    fn serialize(text: &RingSlice, buf: &mut Vec<u8>) {
        buf.put_lenenc_int(text.len() as u64);
        // buf.put_slice(text);
        text.copy_to_vec(buf);
    }

    // fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<RingSlice> {
        let len = buf.parse::<RawInt<VarLen>>(())?;
        // buf.checked_eat(len.0 as usize)
        //     .map(Cow::Borrowed)
        //     .ok_or_else(unexpected_buf_eof)
        buf.checked_eat(len.0 as usize)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// Constantly known byte string.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstBytes<T, const LEN: usize>(PhantomData<T>);

pub trait ConstBytesValue<const LEN: usize> {
    const VALUE: [u8; LEN];
    type Error: Default + std::error::Error + Send + Sync + 'static;
}

impl<T, const LEN: usize> MyDeserialize for ConstBytes<T, LEN>
where
    T: Default,
    T: ConstBytesValue<LEN>,
{
    const SIZE: Option<usize> = Some(LEN);
    type Ctx = ();

    // fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
    fn deserialize((): Self::Ctx, buf: &mut ParseBuf) -> io::Result<Self> {
        let bytes: [u8; LEN] = buf.parse_unchecked(())?;
        if bytes == T::VALUE {
            Ok(Default::default())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                T::Error::default(),
            ))
        }
    }
}

impl<T, const LEN: usize> MySerialize for ConstBytes<T, LEN>
where
    T: ConstBytesValue<LEN>,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        T::VALUE.serialize(buf)
    }
}
