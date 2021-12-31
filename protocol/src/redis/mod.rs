mod command;
mod token;

use std::str::from_utf8;
use token::Token;

use crate::{
    error::ProtocolType, redis::command::PADDING_RSP_TABLE, Command, Commander, Error, Flag,
    HashedCommand, Protocol, RequestProcessor, Result, Stream,
};
use ds::{MemGuard, RingSlice};
use sharding::hash::Hash;

// redis 协议最多支持10w个token
const MAX_TOKEN_COUNT: usize = 100000;
// 最大消息支持1M
const MAX_MSG_LEN: usize = 1000000;

#[derive(Clone, Default)]
pub struct Redis;

impl Redis {
    // 一条redis消息，包含多个token，每个token有2部分，meta部分记录长度信息，数据部分是有效信息。
    // eg：let s = b"*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n";
    // 上面的redis协议，有5个token，分别是mset k1 v1 k2 v2，每个token前面的$len即为meta
    // TODO: 返回的error，如果是ProtocolIncomplete，说明是协议没有读取完毕，后续需要继续读
    #[inline(always)]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        if stream.len() < 4 {
            return Err(Error::ProtocolIncomplete);
        }

        // 解析multibulk count：*5\r\n
        let buf = stream.slice();
        let mut pos = 0;
        if buf.at(pos) as char != '*' {
            return Err(Error::RequestProtocolNotValid);
        }

        log::debug!(
            "+++ will parse req:{:?}",
            from_utf8(buf.to_vec().as_slice())
        );

        pos += 1;
        let len = buf.len();
        let (token_counto, int_len) = parse_len(
            buf.sub_slice(pos, len - pos),
            "multibulk",
            ProtocolType::Request,
        )?;
        let token_count = match token_counto {
            None => 0,
            Some(c) => c,
        };
        pos += int_len;
        if token_count > MAX_TOKEN_COUNT {
            log::warn!("found too long redis req with tokens/{}", token_count);
            return Err(Error::RequestProtocolNotValid);
        }

        debug_assert!(token_count > 0);
        // 解析bulk tokens：$3\r\n123\r\n
        let mut tokens = Vec::with_capacity(token_count);
        for i in 0..token_count {
            if pos >= len {
                return Err(Error::ProtocolIncomplete);
            }
            if buf.at(pos) as char != '$' {
                return Err(Error::RequestProtocolNotValid);
            }
            let meta_pos = pos;
            pos += 1;
            // 注意：meta_left_len是剔除了$的长度
            let (token_leno, meta_left_len) =
                parse_len(buf.sub_slice(pos, len - pos), "bulk", ProtocolType::Request)?;
            let token_len = match token_leno {
                Some(l) => l,
                None => return Err(Error::RequestProtocolNotValid),
            };
            if token_len >= MAX_MSG_LEN {
                return Err(Error::RequestProtocolNotValid);
            }
            pos += meta_left_len;
            let token = Token::from(meta_pos, meta_left_len + 1, pos, token_len);
            tokens.push(token);
            pos += token_len + 2;
            if pos > len || (pos == len && i != (token_count - 1)) {
                return Err(Error::ProtocolIncomplete);
            }
        }

        // cmd的name在第一个str，解析并进行cmd校验
        // TODO: 还有映射的指令，后面再结合eredis整理fishermen
        let cmd_token = tokens.get(0).unwrap();
        //let name_data = cmd_token.bare_data(&buf);
        //let name_data = cmd_token.bare_data(&buf).to_vec();
        //let cmdname = to_str(&name_data, ProtocolType::Request)?;
        let cmdname = cmd_token.bare_data(&buf);
        let prop = command::SUPPORTED.get_by_name(&cmdname)?;
        let last_key_idx = prop.last_key_index(tokens.len());
        let share_tokens_count = tokens.len() - (last_key_idx - prop.first_key_index() + 1);
        prop.validate(tokens.len());

        // 如果没有key，或者key的个数为1，直接执行
        if prop.first_key_index() == 0
            || ((last_key_idx + 1 - prop.first_key_index()) / prop.key_step() == 1)
        {
            let mut key_count = 0;
            let hash;
            if prop.first_key_index() == 0 {
                debug_assert!(prop.operation().is_meta());
                use std::sync::atomic::{AtomicU64, Ordering};
                static RND: AtomicU64 = AtomicU64::new(0);
                hash = RND.fetch_add(1, Ordering::Relaxed) as i64;
            } else {
                let ktoken = tokens.get(prop.first_key_index()).unwrap();
                hash = alg.hash(&ktoken.bare_data(&buf));
                key_count = 1;
            }

            // TODO: flag 还需要针对指令进行进一步设计
            let mut flag = Flag::from_mkey_op(false, prop.padding_rsp(), prop.operation().clone());
            if prop.noforward() {
                flag.set_noforward();
            }
            log::debug!(
                "+++ will process:{:?}",
                from_utf8(buf.sub_slice(0, pos).to_vec().as_slice())
            );
            let guard = stream.take(pos);
            let cmd = HashedCommand::new(guard, hash, flag, key_count);

            // 处理完毕的字节需要take

            // process cmd
            process.process(cmd, true);
            log::debug!("+++ msg processed!");
            return Ok(());
        }

        // 多个key，需要进行分拆

        // 共享第一个token/cmd及第一个key之前的数据，及最后一个key之后的数据
        let first_key_token = tokens.get(prop.first_key_index()).unwrap();
        let last_key_token = tokens.get(last_key_idx).unwrap();
        let prefix = buf
            .sub_slice(
                cmd_token.meta_pos,
                first_key_token.meta_pos - cmd_token.meta_pos,
            )
            .to_vec();
        let suffix = buf
            .sub_slice(last_key_token.end_pos(), len - last_key_token.end_pos())
            .to_vec();
        let first_key_idx = prop.first_key_index();

        // 轮询构建协议，并处理
        if last_key_idx > 20000 {
            log::warn!(
                "too many keys/{} in redis request",
                last_key_idx - first_key_idx + 1
            );
            return Err(Error::ProtocolNotSupported);
        }

        let mut kidx = prop.first_key_index();
        let key_count: u16 = match first_key_idx {
            0 => 0,
            _ => (last_key_idx - first_key_idx + 1) as u16,
        };

        while kidx <= last_key_idx {
            let mut rdata: Vec<u8> = Vec::with_capacity(len);
            // 需要确定出了key之外，其他所有的token都要复制
            rdata.extend(format!("*{}\r\n", share_tokens_count + prop.key_step()).as_bytes());
            // prefix.copy_to_vec(&rdata);
            rdata.extend(prefix.clone());
            let mut j = 0;
            while j < prop.key_step() {
                let token = tokens.get(kidx + j).unwrap();
                rdata.extend(token.bulk_data(&buf).to_vec());
                j += 1;
            }
            if suffix.len() > 0 {
                rdata.extend(suffix.clone());
            }

            let key_token = tokens.get(kidx).unwrap();
            let hash = alg.hash(&key_token.bare_data(&buf));

            log::debug!("+++ will send sub-req:{:?}", from_utf8(rdata.as_slice()));
            let guard = MemGuard::from_vec(rdata);
            // flag 目前包含3个属性：key-count，is-first-key，operation
            let flag: Flag = match kidx == first_key_idx {
                true => Flag::from_mkey_op(true, prop.padding_rsp(), prop.operation().clone()),
                false => Flag::from_mkey_op(false, prop.padding_rsp(), prop.operation().clone()),
            };
            let cmd = HashedCommand::new(guard, hash, flag, key_count);

            // process cmd
            process.process(cmd, kidx == last_key_idx);

            // key处理完毕，跳到下一个key
            kidx += prop.key_step();
        }

        log::debug!(
            "+++ processed req: {:?}",
            from_utf8(buf.to_vec().as_slice())
        );
        // 处理完毕的字节需要take
        stream.take(pos);

        Ok(())
    }

    fn parse_response_inner<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        if data.len() <= 2 {
            return Err(Error::ProtocolIncomplete);
        }
        let response = data.slice();
        log::debug!(
            "+++ will parse rsp:{:?}",
            from_utf8(response.to_vec().as_slice())
        );
        // 响应目前只记录meta前缀长度
        let mut pos = 0;
        match response.at(0) as char {
            '*' => {
                pos += 1;
                let len = response.len();
                // multibulks count
                let (token_counto, meta_left_lenlen) = parse_len(
                    response.sub_slice(pos, len - pos),
                    "bulk",
                    ProtocolType::Response,
                )?;

                let token_count = match token_counto {
                    Some(c) => c,
                    None => 0,
                };
                pos += meta_left_lenlen;

                // 记录meta 长度
                debug_assert!(pos < 256);
                let mut flag = Flag::from_metalen_tokencount(pos as u8, token_count as u8);

                if token_count > 1 {
                    log::error!(
                        "found special resp with tokens/{}: {:?}",
                        token_count,
                        response
                    );
                    return Err(Error::ProtocolNotSupported);
                }

                // 解析并验证bulk tokens：$3\r\n123\r\n
                for i in 0..token_count {
                    if pos >= len {
                        return Err(Error::ProtocolIncomplete);
                    }
                    if response.at(pos) as char != '$' {
                        return Err(Error::ResponseProtocolNotValid);
                    }

                    pos += 1;
                    let (token_leno, meta_left_len) = parse_len(
                        response.sub_slice(pos, len - pos),
                        "bulk",
                        ProtocolType::Response,
                    )?;

                    let token_len = match token_leno {
                        Some(l) => l,
                        None => 0,
                    };
                    if token_len >= MAX_MSG_LEN {
                        log::warn!("careful too long token: {}", token_len);
                    }
                    // 走过$2\r\nab\r\n
                    pos += meta_left_len + token_len;
                    if token_count > 0 {
                        pos += token::REDIS_SPLIT_LEN;
                    }
                    if pos > len || (pos == len && i != token_count - 1) {
                        return Err(Error::ProtocolIncomplete);
                    }
                }

                flag.set_status_ok();
                // 到了这里，response已经解析完毕,对于resp，每个cmd并不知晓自己的key数量是0还是1
                debug_assert!(pos <= len);
                log::debug!(
                    "+++ parsed rsp: {:?}",
                    from_utf8(response.sub_slice(0, pos).to_vec().as_slice())
                );
                return Ok(Some(Command::new(flag, 0, data.take(pos))));
            }
            '$' => {
                // one bulk
                pos += 1;
                let (dataleno, meta_len) = parse_len(
                    response.sub_slice(pos, response.len() - pos),
                    "rsp-bulk",
                    ProtocolType::Response,
                )?;
                let datalen = match dataleno {
                    Some(l) => l,
                    None => 0,
                };
                if datalen > MAX_MSG_LEN {
                    log::warn!("found too long respons/{}", datalen);
                }
                pos += meta_len + datalen;
                if datalen > 0 {
                    // 只有bare len大于0，才会有bare data + \r\n
                    pos += token::REDIS_SPLIT_LEN;
                }
                let mut flag = Flag::from_metalen_tokencount(0, 1u8);
                flag.set_status_ok();
                if pos > response.len() {
                    log::warn!(
                        "+++++ response is incompleted pos:{}, len:{}",
                        pos,
                        response.len()
                    );
                    return Err(Error::ProtocolIncomplete);
                }
                debug_assert!(pos <= data.len());
                log::debug!(
                    "+++ parsed rsp: {:?}",
                    from_utf8(response.sub_slice(0, pos).to_vec().as_slice())
                );
                return Ok(Some(Command::new(flag, 0, data.take(pos))));
            }
            _ => {
                // others
                for i in 1..(response.len() - 1) {
                    if response.at(i) as char == '\r' && response.at(i + 1) as char == '\n' {
                        // i 为pos，+1 为len，再+1到下一个字符\n
                        // let rdata = response.sub_slice(0, i + 1 + 1);
                        let pos = i + 1 + 1;
                        let mut flag = Flag::from_metalen_tokencount(0, 1u8);
                        flag.set_status_ok();
                        debug_assert!(pos <= data.len());
                        log::debug!(
                            "+++ parsed rsp: {:?}",
                            from_utf8(response.sub_slice(0, pos).to_vec().as_slice())
                        );
                        return Ok(Some(Command::new(flag, 0, data.take(pos))));
                    }
                }
                return Err(Error::ProtocolIncomplete);
            }
        }
    }
}

impl Protocol for Redis {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        let mut count = 0;
        loop {
            match self.parse_request_inner(stream, alg, process) {
                Ok(_) => count += 1,
                Err(e) => match e {
                    Error::ProtocolIncomplete => return Ok(()),
                    _ => return Err(e),
                },
            }
            if count > 1000 {
                log::warn!("too big pipeline: {}", count);
            }
        }
    }

    // 为每一个req解析一个response
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        match self.parse_response_inner(data) {
            Ok(cmd) => return Ok(cmd),
            Err(e) => match e {
                Error::ProtocolIncomplete => return Ok(None),
                _ => return Err(e),
            },
        }
    }
    #[inline(always)]
    fn write_response<C: Commander, W: crate::ResponseWriter>(
        &self,
        ctx: &mut C,
        w: &mut W,
    ) -> Result<()> {
        // 首先确认request是否multi-key
        let key_count = ctx.request().key_count();
        let is_mkey_first = match key_count > 1 {
            true => ctx.request().is_mkey_first(),
            false => false,
        };

        // 如果是多个key的req，需要过滤掉每个resp的meta
        let resp = ctx.response();
        let mut oft = 0usize;
        if key_count > 1 {
            // 对于多个key，不管是不是第一个key对应的rsp，都需要去掉resp的meta前缀
            oft = resp.meta_len() as usize;
        }

        let len = resp.len() - oft;

        // 首先发送完整的meta
        // TODO: 1 如果有分片全部不可用，需要构建默认异常响应;
        // TODO: 2 特殊多key的响应 key_count 可能等于token数量？需要确认（理论上不应该存在） fishermen
        if is_mkey_first {
            let meta = format!("*{}\r\n", key_count);
            w.write(meta.as_bytes())?;
        }

        // 发送剩余rsp
        while oft < len {
            let data = resp.read(oft);
            w.write(data)?;
            oft += data.len();
        }
        Ok(())

        // 多个key，第一个response增加multi-bulk-len前缀，后面所有的response去掉bulk-len前缀
    }
    #[inline(always)]
    fn write_no_response<W: crate::ResponseWriter>(
        &self,
        req: &HashedCommand,
        w: &mut W,
    ) -> Result<()> {
        let rsp_idx = req.padding_rsp() as usize;
        debug_assert!(rsp_idx < PADDING_RSP_TABLE.len());
        let rsp = *PADDING_RSP_TABLE.get(rsp_idx).unwrap();
        log::debug!("+++ will write no rsp:{}", rsp);
        w.write(rsp.as_bytes())?;
        Ok(())
    }
}

// 解析bulk长度，起始位置是$2\r\n中的$的下一个元素，所以返回的元组中第二个长度比meta的实际长度小1
fn parse_len(data: RingSlice, name: &str, ptype: ProtocolType) -> Result<(Option<usize>, usize)> {
    if data.len() <= 2 {
        return Err(Error::ProtocolIncomplete);
    }
    let len = data.len();
    let mut idx = 0;
    let mut count = 0;
    let mut count_op = None;
    let invalid_err = match ptype {
        ProtocolType::Request => Error::RequestProtocolNotValid,
        ProtocolType::Response => Error::ResponseProtocolNotValid,
    };
    while data.at(idx) as char != '\r' {
        let c = data.at(idx) as char;
        if c == '-' {
            // 处理 $-1 这种情况
            idx += 1;
            while data.at(idx) as char != '\r' {
                idx += 1;
                if idx >= len {
                    return Err(Error::ProtocolIncomplete);
                }
            }
            count = 0;
            count_op = None;
            log::debug!(
                "+++ found 0 len bulk:{:?}",
                from_utf8(data.to_vec().as_slice())
            );
            break;
        } else if c < '0' || c > '9' {
            log::warn!("found malformed len for {}", name);
            return Err(invalid_err);
        }
        count *= 10;
        count += c as usize - '0' as usize;
        idx += 1;
        if idx >= len {
            return Err(Error::ProtocolIncomplete);
        }
    }

    idx += 1;
    if idx >= len {
        return Err(Error::ProtocolIncomplete);
    }
    if count > 0 {
        count_op = Some(count);
    }
    if data.at(idx) as char != '\n' {
        return Err(invalid_err);
    }
    // 长度包括flag和换行符，如"*123\r\n"是6，“$123\r\n”也是6
    Ok((count_op, idx + 1))
}

//
pub fn to_str(data: &Vec<u8>, ptype: ProtocolType) -> Result<&str> {
    let invalid_err = match ptype {
        ProtocolType::Request => Error::RequestProtocolNotValid,
        ProtocolType::Response => Error::ResponseProtocolNotValid,
    };

    match from_utf8(data.as_slice()) {
        Ok(s) => Ok(s),
        Err(_e) => Err(invalid_err),
    }
}
