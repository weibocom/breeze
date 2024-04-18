use dns_parser::{Error, Header, Name, Opcode, QueryClass, QueryType, ResponseCode, Type};

use metrics::Metric;

use byteorder::{BigEndian, ByteOrder};
use std::net::Ipv4Addr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
pub struct DnsProtocol {
    err: Metric,
    stream: Vec<TcpStream>,
    buf: Vec<u8>,
}

use super::Record;
impl DnsProtocol {
    // 从/etc/resolv.conf中读取第一个nameserver。
    // TODO: 需要支持多个nameserver
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(2048),
            stream: Vec::new(),
            err: metrics::Path::base().qps("lookup_err"),
        }
    }
    pub(super) async fn lookups<'a, I>(&mut self, iter: I) -> (usize, Option<String>)
    where
        I: Iterator<Item = (&'a str, &'a mut Record)>,
    {
        let mut num = 0;
        let mut cache = None;
        for (host, r) in iter {
            match self.lookup(host).await {
                Ok(ips) => {
                    if r.refresh(ips) {
                        num += 1;
                        (num == 1).then(|| cache = Some(host.to_string()));
                    }
                }
                Err(_e) => break,
            }
        }
        (num, cache)
    }
    // 从/etc/resolv.conf中读取nameserver，并连接。
    // 如果无法建立有效连接，尝试3次，每次间隔500ms。
    async fn check(&mut self) -> std::io::Result<()> {
        while self.stream.is_empty() {
            let mut file = tokio::fs::File::open("/etc/resolv.conf").await?;
            let mut buf = String::new();
            file.read_to_string(&mut buf).await?;
            for line in buf.lines().rev() {
                if !line.starts_with("nameserver") {
                    continue;
                }
                if let Some(nameserver) = line.split_whitespace().nth(1) {
                    // 检查是否有端口，没有端口默认为53
                    let nameserver = nameserver
                        .find(':')
                        .map(|_| nameserver.to_string())
                        .unwrap_or_else(|| format!("{}:53", nameserver));
                    if let Ok(s) = TcpStream::connect(&nameserver).await {
                        self.stream.push(s);
                    }
                }
            }
            if self.stream.is_empty() {
                println!("no nameserver established, sleep 1s");
                tokio::time::sleep(ds::time::Duration::from_secs(1)).await;
            }
        }
        // 无法建立连接，返回失败
        Ok(())
    }
    pub async fn lookup(&mut self, host: &str) -> std::io::Result<Ipv4Lookup<'_>> {
        loop {
            self.check().await?;
            assert!(!self.stream.is_empty());
            let Self { stream, buf, .. } = self;
            let s = stream.last_mut().expect("stream check failed");
            match Self::lookup_inner(host, s, buf).await {
                Err(e) => {
                    let dropped = stream.pop();
                    self.err += 1;
                    log::warn!("lookup error:{} {:?}", host, e);
                    println!(
                        "lookup error:{e:?} => {dropped:?} {:?}",
                        dropped.as_ref().map(|s| s.peer_addr())
                    );
                }
                // 前面2个字节是包长度。
                Ok(()) => return Ok(Ipv4Lookup::new(&self.buf[2..], &mut self.err)),
            }
        }
    }
    async fn lookup_inner(
        host: &str,
        stream: &mut TcpStream,
        buf: &mut Vec<u8>,
    ) -> std::io::Result<()> {
        unsafe { buf.set_len(0) };
        let mut query = Query::new(buf);
        query.send(host, stream).await?;
        // 清空buff，读取的时候需要重复使用
        unsafe { buf.set_len(0) };
        let mut answer = Answer::new(buf);
        answer.recv(host, stream).await?;
        Ok(())
    }
}
#[derive(Debug)]
pub struct Query<'a> {
    // [0..2]: packet size
    // [2..14] header
    pub buf: &'a mut Vec<u8>,
}

impl<'a> Query<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        Self { buf }
    }
    pub fn build(&mut self, id: u16, query: &str) {
        assert_eq!(self.buf.len(), 0);
        self.buf.reserve(512);
        // [0..2]: packet size, 先跳过，最后设置
        unsafe { self.buf.set_len(2) };
        self.write_header(id);
        self.add_question(query);
        assert!(self.buf.len() > 2 && self.buf.len() <= 512);
        let packet_len = self.buf.len() as u16 - 2;
        // 前两个字节是整个包的长度
        self.buf[0] = ((packet_len >> 8) & 0xFF) as u8;
        self.buf[1] = (packet_len & 0xFF) as u8;
    }
    fn write_header(&mut self, id: u16) {
        assert!(self.buf.len() == 2, "packet size not reserved");
        let head = Header {
            id,
            query: true,
            opcode: Opcode::StandardQuery,
            authoritative: false,
            truncated: false,
            recursion_desired: true,
            recursion_available: false,
            authenticated_data: false,
            checking_disabled: false,
            response_code: ResponseCode::NoError,
            questions: 0,
            answers: 0,
            nameservers: 0,
            additional: 0,
        };
        // header长度为12
        assert!(self.buf.capacity() >= 128);
        unsafe { self.buf.set_len(2 + 12) };
        head.write(&mut self.buf[2..]);
    }
    fn add_question(&mut self, qname: &str) {
        let prefer_unicast = false;
        // 前2个字节是packet size，不是packet的一部分。
        const OFT: usize = 2;
        assert_eq!(
            &self.buf[OFT + 6..OFT + 12],
            b"\x00\x00\x00\x00\x00\x00",
            "Too late to add a question",
        );
        self.write_name(qname);
        const QT: u16 = QueryType::A as u16;
        const QC: u16 = QueryClass::IN as u16;
        WriteBytesExt::write_u16::<BigEndian>(self.buf, QT).unwrap();
        let unicast: u16 = if prefer_unicast { 0x8000 } else { 0x0000 };
        use byteorder::WriteBytesExt;
        WriteBytesExt::write_u16::<BigEndian>(self.buf, QC | unicast).unwrap();
        let oldq = BigEndian::read_u16(&self.buf[OFT + 4..OFT + 6]);
        assert!(oldq < 65535, "Too many questions");
        BigEndian::write_u16(&mut self.buf[OFT + 4..OFT + 6], oldq + 1);
    }

    fn write_name(&mut self, name: &str) {
        for part in name.split('.') {
            debug_assert!(part.len() < 63);
            let ln = part.len() as u8;
            self.buf.push(ln);
            self.buf.extend(part.as_bytes());
        }
        self.buf.push(0);
    }
    async fn send<S: AsyncWrite + Unpin>(&mut self, host: &str, s: &mut S) -> std::io::Result<()> {
        static ID: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 2;
        self.build(id, host);

        s.write_all(&self.buf[..]).await?;
        s.flush().await?;
        Ok(())
    }
}

impl<'a> Ipv4Lookup<'a> {
    fn new(data: &'a [u8], err: &'a mut Metric) -> Self {
        Self { data, err }
    }
    // 错误不返回
    pub fn visit(&mut self, f: impl FnMut(Ipv4Addr)) {
        let _ = self._visit(f).map_err(|e| {
            *self.err += 1;
            log::warn!("ipv4lookup visit error:{:?}", e);
            println!("ipv4lookup visit error:{:?}", e);
        });
    }
    fn _visit(&mut self, mut f: impl FnMut(Ipv4Addr)) -> Result<(), Error> {
        let header = Header::parse(self.data)?;
        if header.response_code != ResponseCode::NoError {
            return Err(Error::WrongState);
        }
        let mut offset = Header::size();
        // skip questions
        for _ in 0..header.questions {
            let name = Name::scan(&self.data[offset..], self.data)?;
            // skip query type and class
            offset += name.byte_len() + 2 + 2;
            if offset > self.data.len() {
                return Err(Error::UnexpectedEOF);
            }
        }
        for _ in 0..header.answers {
            let (_name, ip) = parse_record(self.data, &mut offset)?;
            if let Some(ip) = ip {
                f(ip);
            }
        }
        Ok(())
    }
}

// Generic function to parse answer, nameservers, and additional records.
fn parse_record<'a>(
    data: &'a [u8],
    offset: &mut usize,
) -> Result<(Name<'a>, Option<Ipv4Addr>), Error> {
    let name = Name::scan(&data[*offset..], data)?;
    *offset += name.byte_len();
    if *offset + 10 > data.len() {
        return Err(Error::UnexpectedEOF);
    }
    let typ = Type::parse(BigEndian::read_u16(&data[*offset..*offset + 2]))?;
    *offset += 2;
    // class code
    *offset += 2;
    // ttl
    *offset += 4;
    let rdlen = BigEndian::read_u16(&data[*offset..*offset + 2]) as usize;
    *offset += 2;
    if *offset + rdlen > data.len() {
        return Err(Error::UnexpectedEOF);
    }
    let data = (typ == Type::A).then(|| Ipv4Addr::from(BigEndian::read_u32(&data[*offset..])));
    *offset += rdlen;
    Ok((name, data))
}

pub struct Ipv4Lookup<'a> {
    data: &'a [u8],
    err: &'a mut Metric,
}

struct Answer<'a> {
    buf: &'a mut Vec<u8>,
}
impl<'a> Answer<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        Self { buf }
    }
    async fn recv<S: AsyncRead + Unpin + std::fmt::Debug>(
        &mut self,
        host: &str,
        s: &mut S,
    ) -> std::io::Result<()> {
        assert!(self.buf.is_empty());
        let mut pkt_len = usize::MAX;
        while self.buf.len() < pkt_len {
            self.buf.reserve(512);
            let available = self.buf.capacity() - self.buf.len();
            let mut buf = unsafe {
                std::slice::from_raw_parts_mut(self.buf.as_mut_ptr().add(self.buf.len()), available)
            };
            let n = s.read(&mut buf).await?;
            if n == 0 {
                println!(
                    "host:{host} pkt_len:{pkt_len}, len:{} response:{:?} s:{s:?}",
                    self.buf.len(),
                    &buf[..n]
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "EOF",
                ));
            }
            if self.buf.len() == 0 && n >= 2 {
                // 第一次需要读取包长度
                pkt_len = ((buf[0] as usize) << 8) | buf[1] as usize;
            }
            unsafe { self.buf.set_len(self.buf.len() + n) };
        }
        Ok(())
    }
}
