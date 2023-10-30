use crate::{
    Command, Commander, Error, Flag, HashedCommand, Metric, MetricItem, Protocol, RequestProcessor,
    Result, Stream, Writer,
};
use ds::RingSlice;
use sharding::hash::Hash;

#[derive(Clone, Default)]
pub struct Vector;

impl Protocol for Vector {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        todo!()
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

pub enum ConditionOP {}
pub struct Condition {
    pub field: RingSlice,
    pub op: ConditionOP,
    pub value: RingSlice,
}

// pub enum Order {
//     ASC,
//     DESC,
// }
// pub struct Orders {
//     pub field: Vec<RingSlice>,
//     pub order: Order,
// }
// pub struct Orders {
//     pub field: RingSliceIter,
//     pub order: Order,
// }

pub struct Limit {
    //无需转成int，可直接拼接
    pub offset: RingSlice,
    pub limit: RingSlice,
}

pub const OP_VRANGE: u16 = 0;
//非迭代版本，代价是内存申请。如果采取迭代版本，需要重复解析一遍，重复解析可以由parser实现，topo调用
pub struct VectorCmd {
    pub keys: Vec<RingSlice>,
    pub fields: RingSlice,
    //三段式条件无需分割，可直接拼接
    pub wheres: Vec<RingSlice>,
    pub orders: RingSlice,
    pub limit: Option<Limit>,
}
