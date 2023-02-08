use crate::{operation::Operation, redis::command::*};

// 默认响应,第0个表示qui;
// bfmget、bfmset在key异常响应时，返回-10作为nil
const PADDING_RSP_TABLE: [&str; 6] = [
    "",
    "+OK\r\n",
    "+PONG\r\n",
    "-ERR phantom no available\r\n",
    "-ERR unknown command\r\n",
    ":-10\r\n",
];

use Operation::*;
type Cmd = CommandProperties;
#[ctor::ctor]
#[rustfmt::skip]
pub(super) static SUPPORTED: Commands = {
    let mut cmds = Commands::new();
    let pt = PADDING_RSP_TABLE;
    // TODO：后续增加新指令时，当multi/need_bulk_num 均为true时，需要在add_support中进行nil转换，避免将err返回到client fishermen
    for c in vec![
        // meta 指令
        
        Cmd::new("command").arity(-1).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("ping").arity(-1).op(Meta).padding(pt[2]).nofwd(),
        // 不支持select 0以外的请求。所有的select请求直接返回，默认使用db0
        Cmd::new("select").arity(2).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("hello").arity(2).op(Meta).padding(pt[4]).nofwd(),
        // quit 的padding应该为1，返回+OK，并断连接
        Cmd::new("quit").arity(2).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("bfget").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("bfset").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        // bfmget、bfmset，padding改为5， fishermen
        Cmd::new("bfmget").m("bfget").arity(-2).op(MGet).first(1).last(-1).step(1).padding(pt[5]).multi().key().bulk(),
        Cmd::new("bfmset").m("bfset").arity(-2).op(Store).first(1).last(1).step(1).padding(pt[5]).multi().key().bulk(),
    ] {
        cmds.add_support(c);
    }
    cmds
};
