use std::mem::transmute;
use std::mem::ManuallyDrop;

use crate::Attachment;
use crate::Command;
use crate::Operation;
use crate::Packet;
use ds::RingSlice;

use super::VectorCmd;

#[repr(C)]
pub struct VectorAttach {
    // type
    attach_type: AttachType,
    // attach basic fields
    pub vcmd: VectorCmd,
    // 查询的轮次，0代表si
    pub round: u16,
    // 最新查询的rsp状态
    pub rsp_ok: bool,

    // attach ext fields
    attach_ext: VectorAttachExt,
}

// TODO retrieveAttach 需要在合适的位置 mannual drop fishermen
pub union VectorAttachExt {
    pub(crate) retrieve_attach: ManuallyDrop<RetrieveAttach>,
    pub(crate) store_attach: StoreAttach,
}

#[derive(Debug)]
#[repr(C)]
pub struct RetrieveAttach {
    // pub vcmd: VectorCmd,
    // pub rsp_ok: bool,
    // // 查询的轮次，0代表si
    // pub round: u16,
    // 待查询数量，不能超过u16::MAX
    pub left_count: u16,
    body_token_count: u16,
    //本轮响应是否成功
    // header，*2 + column names
    header: Vec<u8>,
    body: Vec<Vec<u8>>,
    // 查询响应的body中token数量
    si: Vec<SiItem>, // si表中查询到的数据, si字段信息在配置里存放
}

#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
pub struct StoreAttach {
    op_date: VDate,
    pub(crate) affected_rows: u16,
}

impl VectorAttachExt {
    #[inline]
    pub fn default_retrieve_attach() -> Self {
        Self {
            retrieve_attach: ManuallyDrop::new(RetrieveAttach::default()),
        }
    }

    #[inline]
    pub fn default_store_attach() -> Self {
        Self {
            store_attach: StoreAttach::default(),
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub enum AttachType {
    Unknown = 0,
    Retrieve = 1,
    Store = 2,
}

impl Default for AttachType {
    fn default() -> Self {
        AttachType::Unknown
    }
}

impl VectorAttach {
    #[inline(always)]
    pub fn new(operation: Operation) -> Self {
        match operation {
            Operation::Get | Operation::Gets => Self {
                attach_type: AttachType::Retrieve,
                vcmd: VectorCmd::default(),
                round: 0,
                rsp_ok: false,
                attach_ext: VectorAttachExt::default_retrieve_attach(),
            },
            Operation::Store => Self {
                attach_type: AttachType::Store,
                vcmd: VectorCmd::default(),
                round: 0,
                rsp_ok: false,
                attach_ext: VectorAttachExt::default_store_attach(),
            },
            _ => panic!("only support get/gets/store for kvector"),
        }
    }
    #[inline(always)]
    pub fn from(att: Attachment) -> VectorAttach {
        unsafe { std::mem::transmute(att) }
    }
    #[inline(always)]
    pub fn attach(att: &Attachment) -> &VectorAttach {
        unsafe { std::mem::transmute(att) }
    }
    #[inline(always)]
    pub fn attach_mut(att: &mut Attachment) -> &mut VectorAttach {
        unsafe { std::mem::transmute(att) }
    }
    #[inline(always)]
    pub fn to_attach(self) -> Attachment {
        unsafe { std::mem::transmute(self) }
    }

    #[inline(always)]
    pub fn attch_type(&self) -> &AttachType {
        &self.attach_type
    }

    #[inline(always)]
    pub fn retrieve_attach(&self) -> &RetrieveAttach {
        assert!(self.attach_type.is_retrieve(), "{:?}", self.attach_type);
        unsafe { &self.attach_ext.retrieve_attach }
    }

    #[inline(always)]
    pub fn store_attach(&self) -> &StoreAttach {
        assert!(self.attach_type.is_store());
        unsafe { &self.attach_ext.store_attach }
    }

    #[inline(always)]
    pub fn retrieve_attach_mut(&mut self) -> &mut RetrieveAttach {
        assert!(self.attach_type.is_retrieve(), "{:?}", self.attach_type);
        unsafe { &mut self.attach_ext.retrieve_attach }
    }

    #[inline(always)]
    pub fn store_attach_mut(&mut self) -> &mut StoreAttach {
        assert!(self.attach_type.is_store());
        unsafe { &mut self.attach_ext.store_attach }
    }
}

impl Drop for VectorAttach {
    /// TODO 对于retrieve attach，需要手动进行drop
    fn drop(&mut self) {
        match self.attach_type {
            AttachType::Retrieve => unsafe {
                ManuallyDrop::drop(&mut self.attach_ext.retrieve_attach);
                log::info!("+++ drop retrieve attach manually!");
            },
            _ => {}
        }
    }
}

impl AttachType {
    #[inline(always)]
    pub fn is_retrieve(&self) -> bool {
        match self {
            AttachType::Retrieve => true,
            _ => false,
        }
    }

    #[inline(always)]
    pub fn is_store(&self) -> bool {
        match self {
            AttachType::Store => true,
            _ => false,
        }
    }
}

#[derive(Debug, Default)]
pub struct SiItem {
    pub date: VDate,
    pub count: u16,
}
impl SiItem {
    pub fn new(yy: u8, mm: u8, count: u16) -> Self {
        Self {
            date: VDate {
                year: yy,
                month: mm,
            },
            count,
        }
    }
}
#[derive(Debug, Default, Clone, Copy)]
pub struct VDate {
    pub year: u8,  // year
    pub month: u8, // month
}
impl VDate {
    // 2024-11-01 -> {24, 11}
    pub fn from(d: &RingSlice) -> Self {
        if let Some(first) = d.find(0, b'-') {
            let y = d.try_str_num(0..first).unwrap_or(0);
            if let Some(second) = d.find(first + 1, b'-') {
                let m = d.try_str_num(first + 1..second).unwrap_or(0);
                if y > 0 && m > 0 {
                    return Self {
                        year: y.checked_rem(100).unwrap_or(0) as u8,
                        month: m as u8,
                    };
                }
            }
        }
        Self { year: 0, month: 0 }
    }
    #[inline]
    pub fn year(&self) -> u8 {
        self.year
    }
    #[inline]
    pub fn month(&self) -> u8 {
        self.month
    }
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.month > 0 && self.year > 0
    }
}

pub trait VAttach {
    fn attach(&self) -> &VectorAttach;
    fn attach_mut(&mut self) -> &mut VectorAttach;
}

impl<T: crate::Request> VAttach for T {
    #[inline(always)]
    fn attach(&self) -> &VectorAttach {
        unsafe { transmute(self.attachment().expect("attach is none")) }
    }
    #[inline(always)]
    fn attach_mut(&mut self) -> &mut VectorAttach {
        unsafe { std::mem::transmute(self.attachment_mut().as_mut().expect("attach is none")) }
    }
}

impl Default for RetrieveAttach {
    fn default() -> Self {
        Self {
            left_count: 0,
            header: Vec::with_capacity(8),
            body: Vec::with_capacity(12),
            body_token_count: 0,
            si: Vec::with_capacity(6),
        }
    }
}

impl RetrieveAttach {
    #[inline]
    pub fn with_left_count(&mut self, left_count: u16) {
        // *self = RetrieveAttach {
        //     round: 0,
        //     left_count,
        //     header: Vec::with_capacity(8),
        //     body: Vec::with_capacity(12),
        //     body_token_count: 0,
        //     rsp_ok: false,
        //     si: Vec::with_capacity(6),
        //     vcmd: Default::default(),
        // };
        self.left_count = left_count;
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    // pub fn attach_header(&mut self, header: &mut ResponseHeader) {
    pub fn swap_header_data(&mut self, header: &mut Vec<u8>) {
        // self.header = header;
        std::mem::swap(&mut self.header, header);
    }
    #[inline]
    pub fn attach_body(&mut self, body_data: Vec<u8>, rows: u16, columns: u16) {
        self.body.push(body_data);
        self.body_token_count += rows * columns;
        self.left_count = self.left_count.saturating_sub(rows);
    }

    #[inline]
    pub fn header(&self) -> &Vec<u8> {
        &self.header
    }

    #[inline]
    pub fn body(&self) -> &Vec<Vec<u8>> {
        &self.body
    }

    #[inline]
    pub fn body_token_count(&self) -> u16 {
        self.body_token_count
    }
    // 从response中解析si
    // 约定：si返回结果的结构： uid、date、count顺序排列
    #[inline]
    pub fn attach_si(&mut self, response: &Command) -> bool {
        let header = response.header.as_ref().expect("rsp si");
        let rows = header.rows;
        let cols = header.columns;
        debug_assert_eq!(cols, 3);
        self.si.reserve(rows as usize);
        let data = Packet::from(***response);
        let mut oft: usize = 0;
        while oft < data.len() {
            if data.num(&mut oft).is_err() {
                return false;
            }
            let d = data.bulk_string(&mut oft);
            if d.is_err() {
                return false;
            }
            if let Ok(count) = data.num(&mut oft) {
                if count > 0 {
                    let date = VDate::from(&d.unwrap());
                    if date.is_valid() {
                        let si_item = SiItem::new(date.year(), date.month(), count as u16);
                        self.si.push(si_item);
                    }
                }
            }
        }
        self.si.len() > 0
    }
    #[inline]
    pub fn has_si(&mut self) -> bool {
        self.si.len() > 0
    }
    #[inline]
    pub fn si(&self) -> &Vec<SiItem> {
        &self.si
    }
}

impl StoreAttach {
    #[inline]
    pub fn incr_affected_rows(&mut self, affected_rows: u16) {
        self.affected_rows = self.affected_rows + affected_rows;
    }
}

#[cfg(test)]
mod tests {
    use ds::MemGuard;

    use crate::ResponseHeader;

    use super::*;
    #[test]
    fn test_attach_si() {
        let header: ResponseHeader = ResponseHeader::new(
            "*3\r\n$3\r\nuid\r\n$10\r\nstart_date\r\n$5\r\ncount\r\n".into(),
            246,
            3,
        );
        let body: MemGuard = MemGuard::from_vec(":6351590999\r\n$10\r\n2024-06-01\r\n:674\r\n:6351590999\r\n$10\r\n2024-05-01\r\n:1113\r\n:6351590999\r\n$10\r\n2024-04-01\r\n:833\r\n:6351590999\r\n$10\r\n2024-03-01\r\n:45\r\n:6351590999\r\n$10\r\n2024-02-01\r\n:61\r\n:6351590999\r\n$10\r\n2024-01-01\r\n:59\r\n:6351590999\r\n$10\r\n2023-12-01\r\n:20\r\n:6351590999\r\n$10\r\n2023-11-01\r\n:9\r\n:6351590999\r\n$10\r\n2023-10-01\r\n:13\r\n:6351590999\r\n$10\r\n2023-09-01\r\n:50\r\n:6351590999\r\n$10\r\n2023-08-01\r\n:16\r\n:6351590999\r\n$10\r\n2023-07-01\r\n:61\r\n:6351590999\r\n$10\r\n2023-06-01\r\n:30\r\n:6351590999\r\n$10\r\n2023-05-01\r\n:41\r\n:6351590999\r\n$10\r\n2023-04-01\r\n:54\r\n:6351590999\r\n$10\r\n2023-03-01\r\n:108\r\n:6351590999\r\n$10\r\n2023-02-01\r\n:213\r\n:6351590999\r\n$10\r\n2023-01-01\r\n:159\r\n:6351590999\r\n$10\r\n2022-12-01\r\n:26\r\n:6351590999\r\n$10\r\n2022-11-01\r\n:16\r\n:6351590999\r\n$10\r\n2022-10-01\r\n:14\r\n:6351590999\r\n$10\r\n2022-09-01\r\n:3\r\n:6351590999\r\n$10\r\n2022-08-01\r\n:10\r\n:6351590999\r\n$10\r\n2022-07-01\r\n:9\r\n:6351590999\r\n$10\r\n2022-06-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-05-01\r\n:23\r\n:6351590999\r\n$10\r\n2022-04-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-03-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-02-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-01-01\r\n:5\r\n:6351590999\r\n$10\r\n2021-12-01\r\n:14\r\n:6351590999\r\n$10\r\n2021-11-01\r\n:4\r\n:6351590999\r\n$10\r\n2021-10-01\r\n:2\r\n:6351590999\r\n$10\r\n2021-09-01\r\n:3\r\n:6351590999\r\n$10\r\n2021-08-01\r\n:25\r\n:6351590999\r\n$10\r\n2021-07-01\r\n:36\r\n:6351590999\r\n$10\r\n2021-06-01\r\n:30\r\n:6351590999\r\n$10\r\n2021-05-01\r\n:18\r\n:6351590999\r\n$10\r\n2021-04-01\r\n:20\r\n:6351590999\r\n$10\r\n2021-03-01\r\n:21\r\n:6351590999\r\n$10\r\n2021-02-01\r\n:35\r\n:6351590999\r\n$10\r\n2021-01-01\r\n:22\r\n:6351590999\r\n$10\r\n2020-12-01\r\n:55\r\n:6351590999\r\n$10\r\n2020-11-01\r\n:22\r\n:6351590999\r\n$10\r\n2020-10-01\r\n:37\r\n:6351590999\r\n$10\r\n2020-09-01\r\n:33\r\n:6351590999\r\n$10\r\n2020-08-01\r\n:15\r\n:6351590999\r\n$10\r\n2020-07-01\r\n:12\r\n:6351590999\r\n$10\r\n2020-06-01\r\n:26\r\n:6351590999\r\n$10\r\n2020-05-01\r\n:54\r\n:6351590999\r\n$10\r\n2020-04-01\r\n:38\r\n:6351590999\r\n$10\r\n2020-03-01\r\n:27\r\n:6351590999\r\n$10\r\n2020-02-01\r\n:80\r\n:6351590999\r\n$10\r\n2020-01-01\r\n:99\r\n:6351590999\r\n$10\r\n2019-12-01\r\n:67\r\n:6351590999\r\n$10\r\n2019-11-01\r\n:120\r\n:6351590999\r\n$10\r\n2019-10-01\r\n:80\r\n:6351590999\r\n$10\r\n2019-09-01\r\n:76\r\n:6351590999\r\n$10\r\n2019-08-01\r\n:120\r\n:6351590999\r\n$10\r\n2019-07-01\r\n:140\r\n:6351590999\r\n$10\r\n2019-06-01\r\n:118\r\n:6351590999\r\n$10\r\n2019-05-01\r\n:146\r\n:6351590999\r\n$10\r\n2019-04-01\r\n:287\r\n:6351590999\r\n$10\r\n2019-03-01\r\n:83\r\n:6351590999\r\n$10\r\n2019-02-01\r\n:88\r\n:6351590999\r\n$10\r\n2019-01-01\r\n:262\r\n:6351590999\r\n$10\r\n2018-12-01\r\n:213\r\n:6351590999\r\n$10\r\n2018-11-01\r\n:251\r\n:6351590999\r\n$10\r\n2018-10-01\r\n:215\r\n:6351590999\r\n$10\r\n2018-09-01\r\n:192\r\n:6351590999\r\n$10\r\n2018-08-01\r\n:208\r\n:6351590999\r\n$10\r\n2018-07-01\r\n:339\r\n:6351590999\r\n$10\r\n2018-06-01\r\n:97\r\n:6351590999\r\n$10\r\n2018-05-01\r\n:162\r\n:6351590999\r\n$10\r\n2018-04-01\r\n:127\r\n:6351590999\r\n$10\r\n2018-03-01\r\n:147\r\n:6351590999\r\n$10\r\n2018-02-01\r\n:529\r\n:6351590999\r\n$10\r\n2018-01-01\r\n:702\r\n:6351590999\r\n$10\r\n2017-12-01\r\n:453\r\n:6351590999\r\n$10\r\n2017-11-01\r\n:70\r\n:6351590999\r\n$10\r\n2017-10-01\r\n:1\r\n:6351590999\r\n$10\r\n2017-08-01\r\n:1\r\n".into());
        let response: Command = Command::with_assemble_pack(true, header, body);
        let mut att = RetrieveAttach::default();
        att.with_left_count(1);
        let r = att.attach_si(&response);
        assert!(r);
    }
}
