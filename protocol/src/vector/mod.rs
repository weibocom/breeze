mod command;
pub mod flager;
mod reqpacket;

use crate::{
    Command, Commander, Error, HashedCommand, Metric, MetricItem, Protocol, RequestProcessor,
    Result, Stream, Writer,
};
use ds::RingSlice;
use sharding::hash::Hash;

use self::reqpacket::RequestPacket;

#[derive(Clone, Default)]
pub struct Vector;

impl Protocol for Vector {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        // 解析redis格式的指令，构建结构化的redis vector 内部cmd，要求：方便转换成sql
        let mut packet = RequestPacket::new(stream);
        match self.parse_request_inner(&mut packet, alg, process) {
            Ok(_) => Ok(()),
            Err(Error::ProtocolIncomplete) => {
                // 如果解析数据不够，提前reserve stream的空间
                packet.reserve_stream_buff();
                Ok(())
            }
            e => {
                log::warn!("redis parsed err: {:?}, req: {:?} ", e, packet.inner_data());
                e
            }
        }
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        todo!()
    }

    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> Result<()>
    where
        W: Writer,
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        todo!()
    }
}

impl Vector {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        packet: &mut RequestPacket<S>,
        _alg: &H,
        process: &mut P,
    ) -> Result<()> {
        log::debug!("+++ rec kvector req:{:?}", packet.inner_data());
        while packet.available() {
            packet.parse_bulk_num()?;
            let flag = packet.parse_cmd()?;
            // 构建cmd，准备后续处理
            let cmd = packet.take();
            let hash = 0;
            let cmd = HashedCommand::new(cmd, hash, flag);
            process.process(cmd, true);
        }

        Ok(())
    }
}

// pub struct RingSliceIter {}
// impl Iterator for RingSliceIter {
//     type Item = RingSlice;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }
// pub struct ConditionIter {}
// impl Iterator for ConditionIter {
//     type Item = Condition;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

pub enum Opcode {}

pub(crate) const COND_ORDER: &str = "ORDER";
pub(crate) const COND_LIMIT: &str = "LIMIT";

#[derive(Debug, Clone, Default)]
pub struct Condition {
    pub field: RingSlice,
    pub op: RingSlice,
    pub value: RingSlice,
}

impl Condition {
    /// 构建一个condition
    #[inline]
    pub(crate) fn new(field: RingSlice, op: RingSlice, value: RingSlice) -> Self {
        Self { field, op, value }
    }
}

// pub enum Order {
//     ASC,
//     DESC,
// }
#[derive(Debug, Clone, Default)]
pub struct Order {
    pub field: RingSlice,
    pub order: RingSlice,
}
// pub struct Orders {
//     pub field: RingSliceIter,
//     pub order: Order,
// }

#[derive(Debug, Clone, Default)]
pub struct Limit {
    //无需转成int，可直接拼接
    pub offset: RingSlice,
    pub limit: RingSlice,
}

pub const OP_VRANGE: u16 = 0;
//非迭代版本，代价是内存申请。如果采取迭代版本，需要重复解析一遍，重复解析可以由parser实现，topo调用
#[derive(Debug, Clone, Default)]
pub struct VectorCmd {
    pub keys: Vec<RingSlice>,
    pub fields: Vec<(RingSlice, RingSlice)>,
    //三段式条件无需分割，可直接拼接
    pub wheres: Vec<Condition>,
    pub order: Order,
    pub limit: Limit,
}
