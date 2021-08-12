#[allow(dead_code)]
pub enum PacketPos {
    Magic = 0,
    Opcode = 1,
    Key = 2,
    ExtrasLength = 4,
    DataType = 5,
    Status = 6,
    TotalBodyLength = 8,
    Opaque = 12,
    Cas = 16,
}
