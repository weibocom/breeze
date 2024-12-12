use std::fmt::{Display, Write};

use crate::kv::common::Command;
use crate::kv::{MysqlBinary, VectorSqlBuilder};
use crate::vector::{CommandType, Condition, Field, VectorCmd};
use crate::{Error, Result};
use chrono::{Datelike, NaiveDate};
use ds::RingSlice;

use super::{KeysType, Strategy};

struct VRingSlice<'a>(&'a RingSlice);
impl<'a> Display for VRingSlice<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (s1, s2) = self.0.data();
        f.write_str(unsafe { std::str::from_utf8_unchecked(s1) })?;
        f.write_str(unsafe { std::str::from_utf8_unchecked(s2) })?;
        Ok(())
    }
}

struct Keys<'a>(&'a RingSlice);
impl<'a> Display for Keys<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //todo: 改成新dev的实现，或者提供一个iter
        // let mut start = 0;
        // while let Some(end) = self.0.find(start, b'|') {
        //     if start == 0 {
        //         let _ = write!(f, "{}", VRingSlice(&self.0.slice(start, end - start)));
        //     } else {
        //         let _ = write!(f, ",{}", VRingSlice(&self.0.slice(start, end - start)));
        //     }
        //     start = end + 1;
        // }
        // let left = self.0.len() - start;
        // assert!(left > 0);
        // if start == 0 {
        //     let _ = write!(f, "{}", VRingSlice(&self.0.slice(start, left)));
        // } else {
        //     let _ = write!(f, ",{}", VRingSlice(&self.0.slice(start, left)));
        // }
        VRingSlice(self.0).fmt(f)
    }
}

struct Key<'a>(&'a RingSlice);
impl<'a> Display for Key<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{}`", VRingSlice(self.0))
    }
}

struct Val<'a>(&'a RingSlice);
impl<'a> Display for Val<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = f.write_char('\'');
        self.0.visit(|c| crate::kv::escape_mysql_and_push(f, c));
        let _ = f.write_char('\'');
        Ok(())
    }
}

struct ConditionDisplay<'a>(&'a Condition);
impl<'a> Display for ConditionDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.op.equal(b"in") {
            let _ = write!(
                f,
                "{} {} ({})",
                Key(&self.0.field),
                VRingSlice(&self.0.op),
                //todo:暂时不转义
                VRingSlice(&self.0.value)
            );
        } else {
            let _ = write!(
                f,
                "{}{}{}",
                Key(&self.0.field),
                VRingSlice(&self.0.op),
                Val(&self.0.value)
            );
        }
        Ok(())
    }
}

struct Select<'a>(Option<&'a Field>);
impl<'a> Display for Select<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(field) => Keys(&field.1).fmt(f),
            None => f.write_char('*'),
        }
    }
}

struct Table<'a, S>(&'a S, &'a NaiveDate, i64);
impl<'a, S: Strategy> Display for Table<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write_database_table(f, &self.1, self.2);
        Ok(())
    }
}

struct InsertCols<'a, S>(&'a S, &'a Vec<Field>);
impl<'a, S: Strategy> Display for InsertCols<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, fields) = self;
        for (i, key) in (&mut strategy.keys_with_type()).enumerate() {
            if let KeysType::Keys(key) = key {
                if i == 0 {
                    let _ = write!(f, "`{}`", key);
                } else {
                    let _ = write!(f, ",`{}`", key);
                }
            }
        }
        for field in fields {
            let _ = write!(f, ",{}", Key(&field.0));
        }
        Ok(())
    }
}

struct InsertVals<'a, S>(&'a S, &'a Vec<RingSlice>, &'a Vec<Field>);
impl<'a, S: Strategy> Display for InsertVals<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, keys, fields) = self;
        for (i, key) in (&mut strategy.keys_with_type()).enumerate() {
            if let KeysType::Keys(_) = key {
                if i == 0 {
                    let _ = write!(f, "{}", Val(&keys[i]));
                } else {
                    let _ = write!(f, ",{}", Val(&keys[i]));
                }
            }
        }
        for field in fields {
            let _ = write!(f, ",{}", Val(&field.1));
        }
        Ok(())
    }
}

struct UpdateFields<'a>(&'a Vec<Field>);
impl<'a> Display for UpdateFields<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, field) in self.0.iter().enumerate() {
            if i == 0 {
                let _ = write!(f, "{}={}", Key(&field.0), Val(&field.1));
            } else {
                let _ = write!(f, ",{}={}", Key(&field.0), Val(&field.1));
            }
        }
        Ok(())
    }
}

struct KeysAndCondsAndOrderAndLimit<'a, S>(&'a S, &'a VectorCmd, u64);
impl<'a, S: Strategy> Display for KeysAndCondsAndOrderAndLimit<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(
            strategy,
            vcmd @ VectorCmd {
                cmd,
                route: _,
                keys,
                fields: _,
                wheres,
                order,
                limit,
                group_by,
            },
            extra,
        ) = self;

        // 对于vupdate、si/timeline的vdel，必须得有where语句
        // kv的vdel不需要where语句，因为kv的vdel是根据key来删除的
        match cmd {
            CommandType::VUpdate
            | CommandType::VUpdateSi
            | CommandType::VDelSi
            | CommandType::VUpdateTimeline
            | CommandType::VDelTimeline => {
                if wheres.len() == 0 {
                    log::warn!("+++ ignore vdel/vupdate without where condition");
                    return Err(std::fmt::Error);
                }
            }
            _ => {}
        }

        // 相比多个key
        let mkey_num = keys.len() - strategy.keys().len();
        for (i, key) in (&mut strategy.keys_with_type()).enumerate() {
            if let KeysType::Keys(key) = key {
                if i == 0 {
                    if mkey_num == 0 {
                        let _ = write!(f, "`{}`={}", key, Val(&keys[i]));
                    } else {
                        debug_assert!(*cmd == CommandType::VDel || *cmd == CommandType::VGet || *cmd == CommandType::VRange);
                        for j in 0..(mkey_num+1) {
                            if j == 0 {
                                let _ = write!(f, "`{}` in ({}", key, Val(&keys[i+j]));
                            } else if j == mkey_num {
                                let _ = write!(f, ",{})", Val(&keys[i+j]));
                            } else {
                                let _ = write!(f, ",{}", Val(&keys[i+j]));
                            }
                        }
                    }
                } else {
                    let _ = write!(f, " and `{}`={}", key, Val(&keys[i+mkey_num]));
                }
            }
        }
        for w in wheres {
            let _ = write!(f, " and {}", ConditionDisplay(w));
        }
        if group_by.fields.len() != 0 {
            let _ = write!(f, " group by {}", Keys(&group_by.fields));
        }
        if order.field.len() != 0 {
            let _ = write!(
                f,
                " order by {} {}",
                Keys(&order.field),
                VRingSlice(&order.order)
            );
        }
        let strategy_limit = strategy.batch(extra, vcmd);
        if strategy_limit > 0 {
            let _ = write!(f, " limit {}", strategy.batch(extra, vcmd),);
        } else if limit.offset.len() != 0 {
            let _ = write!(
                f,
                " limit {} offset {}",
                VRingSlice(&limit.limit),
                VRingSlice(&limit.offset)
            );
        }
        Ok(())
    }
}

pub struct SqlBuilder<'a, S> {
    vcmd: &'a VectorCmd,
    hash: i64,
    date: NaiveDate,
    strategy: &'a S,
    limit: u64,
}

impl<'a, S: Strategy> SqlBuilder<'a, S> {
    pub fn new(
        vcmd: &'a VectorCmd,
        hash: i64,
        date: NaiveDate,
        strategy: &'a S,
        limit: u64,
    ) -> Result<Self> {
        Ok(Self {
            vcmd,
            hash,
            date,
            strategy,
            limit,
        })
    }
}

impl<'a, S> MysqlBinary for SqlBuilder<'a, S> {
    fn mysql_cmd(&self) -> Command {
        Command::COM_QUERY
    }
}

impl<'a, S: Strategy> VectorSqlBuilder for SqlBuilder<'a, S> {
    fn len(&self) -> usize {
        //按照可能的最长长度计算，其中table长度取得32，key长度取得5，测试比实际长15左右
        let mut base = match self.vcmd.cmd {
            CommandType::VRange | CommandType::VGet | CommandType::VRangeTimeline => {
                "select  from  where ".len()
            }
            CommandType::VCard => "select count(*) from  where ".len(),
            CommandType::VAdd | CommandType::VAddTimeline => "insert into  () values ()".len(),
            CommandType::VUpdate | CommandType::VUpdateTimeline => "update  set  where ".len(),
            CommandType::VDel | CommandType::VDelTimeline => "delete from  where ".len(),
            CommandType::VRangeSi
            | CommandType::VAddSi
            | CommandType::VUpdateSi
            | CommandType::VDelSi
            | CommandType::Unknown => {
                //校验应该在parser_req出
                panic!("not support in timeline cmd_type:{:?}", self.vcmd.cmd);
            }
        };
        let VectorCmd {
            cmd: _,
            route: _,
            keys,
            fields,
            wheres,
            order,
            limit,
            group_by,
        } = &self.vcmd;
        //key通常只用来构建where条件，8代表"field=''"，包含包裹val的引号
        keys.iter().for_each(|k| base += k.len() + 8);
        fields
            .iter()
            .for_each(|k| base += k.0.len() + k.1.len() + "``=''".len());
        wheres
            .iter()
            .for_each(|k| base += k.field.len() + k.op.len() + k.value.len() + " and ``''".len());
        if order.field.len() != 0 {
            base += order.field.len() + order.order.len() + " order by ".len();
        }
        if limit.limit.len() != 0 {
            base += limit.limit.len() + limit.offset.len() + " limit offset ".len();
        }
        if group_by.fields.len() != 0 {
            base += " group by ".len() + group_by.fields.len();
        }
        base += 32; //tablename
        base.max(64)
    }

    fn write_sql(&self, buf: &mut impl Write) -> crate::Result<()> {
        // TODO：使用所有commandType的具体类型，而非通配符_,避免新增type后，因未变更没有正确处理；后续考虑优化 fishermen
        match self.vcmd.cmd {
            CommandType::VRange | CommandType::VGet | CommandType::VRangeTimeline => {
                let _ = write!(
                    buf,
                    "select {} from {} where {}",
                    Select(self.vcmd.fields.get(0)),
                    Table(self.strategy, &self.date, self.hash),
                    KeysAndCondsAndOrderAndLimit(self.strategy, &self.vcmd, self.limit),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VCard => {
                let _ = write!(
                    buf,
                    "select count(*) from {} where {}",
                    Table(self.strategy, &self.date, self.hash),
                    KeysAndCondsAndOrderAndLimit(self.strategy, &self.vcmd, self.limit),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VAdd | CommandType::VAddTimeline => {
                let _ = write!(
                    buf,
                    "insert into {} ({}) values ({})",
                    Table(self.strategy, &self.date, self.hash),
                    InsertCols(self.strategy, &self.vcmd.fields),
                    InsertVals(self.strategy, &self.vcmd.keys, &self.vcmd.fields),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VUpdate | CommandType::VUpdateTimeline => {
                let _ = write!(
                    buf,
                    "update {} set {} where {}",
                    Table(self.strategy, &self.date, self.hash),
                    UpdateFields(&self.vcmd.fields),
                    KeysAndCondsAndOrderAndLimit(self.strategy, &self.vcmd, self.limit),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VDel | CommandType::VDelTimeline => {
                let _ = write!(
                    buf,
                    "delete from {} where {}",
                    Table(self.strategy, &self.date, self.hash),
                    KeysAndCondsAndOrderAndLimit(self.strategy, &self.vcmd, self.limit),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VRangeSi
            | CommandType::VAddSi
            | CommandType::VUpdateSi
            | CommandType::VDelSi
            | CommandType::Unknown => {
                //校验应该在parser_req出
                panic!("not support in timeline cmd_type:{:?}", self.vcmd.cmd);
            }
        }
        Ok(())
    }
}

pub struct SiSqlBuilder<'a, S> {
    vcmd: &'a VectorCmd,
    hash: i64,
    strategy: &'a S,
    date: NaiveDate,
}

impl<'a, S: Strategy> SiSqlBuilder<'a, S> {
    pub fn new(vcmd: &'a VectorCmd, hash: i64, date: NaiveDate, strategy: &'a S) -> Result<Self> {
        Ok(Self {
            vcmd,
            hash,
            strategy,
            date,
        })
    }
}

impl<'a, S> MysqlBinary for SiSqlBuilder<'a, S> {
    fn mysql_cmd(&self) -> Command {
        Command::COM_QUERY
    }
}

impl<'a, S: Strategy> VectorSqlBuilder for SiSqlBuilder<'a, S> {
    fn len(&self) -> usize {
        128
    }

    // (1) 根据object_type查用户的si数据
    // select uid, start_date as stat_date, sum(count) as count from $db$.$tb$ where uid=? and object_type in(?) group by uid, start_date order by start_date desc

    // (2) 查用户所有的si数据
    // select uid, start_date as stat_date, sum(count) as count from $db$.$tb$ where uid=? group by start_date order by start_date desc

    // select date，count字段名，
    // 条件需要key，字段名

    fn write_sql(&self, buf: &mut impl Write) -> crate::Result<()> {
        match self.vcmd.cmd {
            CommandType::VRange | CommandType::VRangeSi | CommandType::VGet => {
                let _ = write!(
                    buf,
                    "select {} from {} where {}",
                    SiSelect(self.strategy.keys(), self.strategy.si_cols()),
                    SiTable(self.strategy, self.hash),
                    SiKeysAndCondsAndOrder(self.strategy, &self.vcmd),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            // 1.1. 更新si： insert into $db$.$tb$ (uid, object_type, start_date, count) values (?, ?, ?, 1) on duplicate key update count=greatest(0, cast(count as signed) + 1)。
            // 1.2. 根据设置更新timeline：insert into $db$.$tb$ (uid, object_type, like_id, object_id) values (?, ?, ?, ?)
            // 备注：有些场景只更新si，有些场景只更新timeline，需要业务修改时考虑。
            // 1.3. mesh cmd: vadd $uid,$date object_type $obj_type object_id $obj_id like_id $like_id
            CommandType::VAdd | CommandType::VAddSi => {
                let count_col_name = self.strategy.si_cols().last().unwrap();
                //对si表的更新插入至少需要keys + count + counttype 这些字段，下面会兜底校验
                let si_insert_vals = SiInsertVals(
                    self.strategy,
                    &self.date,
                    &self.vcmd.keys,
                    &self.vcmd.fields,
                );
                let count = si_insert_vals.count_in_field();
                write!(
                    buf,
                    "insert into {} ({}) values ({}) on duplicate key update {}=greatest(0, cast({} as signed) + {})",
                    SiTable(self.strategy, self.hash),
                    SiInsertCols(self.strategy, &self.vcmd.fields),
                    si_insert_vals,
                    count_col_name,
                    count_col_name,
                    count
                ).map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VUpdate | CommandType::VUpdateSi => {
                let _ = write!(
                    buf,
                    "update {} set {} where {}",
                    SiTable(self.strategy, self.hash),
                    SiUpdateFields(self.strategy, &self.vcmd.fields),
                    SiKeysAndUpdateOrDelConds(self.strategy, &self.vcmd, &self.date),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            // 2.1. 更新si：update $db$.$tb$ set count = greatest(0,cast(count as signed) - 1) where uid = ? and object_type = ? and start_date = ?
            // 2.2. 删除timeline：delete from $db$.$tb$ where uid=? and object_id=?
            // 备注：有些场景只更新si，有些场景只更新timeline，需要业务修改时考虑。
            // 2.3. mesh cmd: vdel $uid,$date where object_type = $obj_type object_id = $obj_id
            CommandType::VDel | CommandType::VDelSi => {
                //对si表的更新插入至少需要keys + count + counttype 这些字段，下面会兜底校验
                write!(
                    buf,
                    "update {} set count = greatest(0,cast(count as signed) - 1) where {}",
                    SiTable(self.strategy, self.hash),
                    SiKeysAndUpdateOrDelConds(self.strategy, &self.vcmd, &self.date),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VCard => {
                let fields = self.vcmd.fields.first().map_or(None, |f| Some(f));
                let _ = write!(
                    buf,
                    "select {} from {} where {}",
                    SiCardCols(self.strategy, fields),
                    SiTable(self.strategy, self.hash),
                    SiKeysAndConds(self.strategy, &self.vcmd),
                )
                .map_err(|_| Error::RequestProtocolInvalid)?;
            }
            CommandType::VRangeTimeline
            | CommandType::VAddTimeline
            | CommandType::VUpdateTimeline
            | CommandType::VDelTimeline
            | CommandType::Unknown => {
                //校验应该在parser_req出
                panic!("not support cmd_type:{:?}", self.vcmd.cmd);
            }
        };
        Ok(())
    }
}

//keys, cols
struct SiSelect<'a>(&'a [String], &'a [String]);
impl<'a> Display for SiSelect<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // select key, start_date, sum(count)
        write!(
            f,
            "{},{},sum({})",
            self.0[0],
            self.1[0],
            self.1.last().unwrap()
        )
    }
}

struct SiKeysAndConds<'a, S>(&'a S, &'a VectorCmd);
impl<'a, S: Strategy> Display for SiKeysAndConds<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, VectorCmd { keys, wheres, .. }) = self;
        let key_name = &strategy.keys()[0];
        let cols = strategy.si_cols();
        let _ = write!(f, "`{}`={}", key_name, Val(&keys[0]));
        for w in wheres {
            //条件中和si相同的列写入条件
            for col in cols {
                if w.field.equal(col.as_bytes()) {
                    let _ = write!(f, " and {}", ConditionDisplay(w));
                    break;
                }
            }
        }
        Ok(())
    }
}

struct SiKeysAndCondsAndOrder<'a, S>(&'a S, &'a VectorCmd);
impl<'a, S: Strategy> Display for SiKeysAndCondsAndOrder<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, VectorCmd { keys, wheres, .. }) = self;
        let key_name = &strategy.keys()[0];
        let cols = strategy.si_cols();
        let _ = write!(f, "`{}`={}", key_name, Val(&keys[0]));
        for w in wheres {
            //条件中和si相同的列写入条件
            for col in cols {
                if w.field.equal(col.as_bytes()) {
                    let _ = write!(f, " and {}", ConditionDisplay(w));
                    break;
                }
            }
        }
        //按key和日期group，按日期倒叙排
        let _ = write!(
            f,
            " group by {},{} order by {} desc",
            key_name, cols[0], cols[0]
        );
        Ok(())
    }
}

struct SiTable<'a, S>(&'a S, i64);
impl<'a, S: Strategy> Display for SiTable<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write_si_database_table(f, self.1);
        Ok(())
    }
}

struct SiCardCols<'a, S>(&'a S, Option<&'a Field>);
// 如果有非count计数的field，则同时返回这些field，如果有count计数的field，则只返回sum(count) as count的计数
// 目前配置只支持一个count（带type）
impl<'a, S: Strategy> Display for SiCardCols<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, fields_origin) = self;
        let si_cols = strategy.si_cols();
        if let Some(field) = fields_origin {
            if field.1.equal_ignore_case(si_cols[1].as_bytes()) {
                let _ = write!(f, "{},sum({}) as {}", si_cols[1], si_cols[2], si_cols[2]);
            } else {
                return Err(std::fmt::Error);
            }
        } else {
            let _ = write!(f, "sum({}) as {}", si_cols[2], si_cols[2]);
        }
        Ok(())
    }
}

struct SiInsertCols<'a, S>(&'a S, &'a Vec<Field>);
impl<'a, S: Strategy> Display for SiInsertCols<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, fields) = self;
        for (i, key) in strategy.keys_with_type().enumerate() {
            if let KeysType::Keys(key) = key {
                if i == 0 {
                    let _ = write!(f, "`{}`", key);
                } else {
                    let _ = write!(f, ",`{}`", key);
                }
            }
        }
        let mut has_count_type = false;
        let si_cols = strategy.si_cols();
        for field in fields {
            // 目前配置限制了count只能支持1种类型，先打通，后续需要调整配置，以支持多种count fishermen
            // for col in si_cols {
            //     if field.0.equal(col.as_bytes()) {
            //         let _ = write!(f, ",{}", Key(&field.0));
            //         has_count_type = true;
            //     }

            assert_eq!(si_cols.len(), 3);
            if field.0.equal(si_cols[1].as_bytes()) {
                let _ = write!(f, ",{}", Key(&field.0));
                has_count_type = true;
                break;
            }
        }
        //必须提供count_type
        if si_cols.len() > 2 && !has_count_type {
            return Err(std::fmt::Error);
        }
        //date,count
        let _ = write!(
            f,
            ",{},{}",
            si_cols.first().unwrap(),
            si_cols.last().unwrap()
        );
        Ok(())
    }
}

struct SiInsertVals<'a, S>(&'a S, &'a NaiveDate, &'a Vec<RingSlice>, &'a Vec<Field>);
impl<'a, S: Strategy> SiInsertVals<'a, S> {
    pub(crate) fn count_in_field(&self) -> i32 {
        let &Self(strategy, _date, _keys, fields) = self;
        let si_cols = strategy.si_cols();
        for field in fields {
            // 目前配置限制了count只能支持1种类型，先打通，后续需要调整配置，以支持多种count fishermen
            // si cols 当前格式：stat_date,type,count
            assert_eq!(si_cols.len(), 3);
            if field.0.equal(si_cols[2].as_bytes()) {
                let count_slice = field.1;
                if count_slice.at(0) == '-' as u8 {
                    return -1 * (count_slice.str_num(1..) as i32);
                } else {
                    return count_slice.str_num(..) as i32;
                }
            }
        }
        return 1;
    }
}
impl<'a, S: Strategy> Display for SiInsertVals<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, date, keys, fields) = self;
        for (i, key) in (&mut strategy.keys_with_type()).enumerate() {
            if let KeysType::Keys(_) = key {
                if i == 0 {
                    let _ = write!(f, "{}", Val(&keys[i]));
                } else {
                    let _ = write!(f, ",{}", Val(&keys[i]));
                }
            }
        }
        let si_cols = strategy.si_cols();
        let count = self.count_in_field();
        for field in fields {
            // 目前配置限制了count只能支持1种类型，先打通，后续需要调整配置，以支持多种count fishermen
            // si cols 当前格式：stat_date,type,count
            assert_eq!(si_cols.len(), 3);
            if field.0.equal(si_cols[1].as_bytes()) {
                let _ = write!(f, ",{}", Val(&field.1));
                break;
            }
        }
        //date,count
        // 写startdate的格式都是统一按照YY-mm-dd写的
        let _ = write!(
            f,
            ",'{}-{}-{}',{}",
            date.year(),
            date.month(),
            date.day(),
            count
        );
        Ok(())
    }
}

struct SiUpdateFields<'a, S: Strategy>(&'a S, &'a Vec<Field>);
impl<'a, S: Strategy> Display for SiUpdateFields<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, fields) = self;
        // 首先对于count增减，其次对其他字段进行直接设置
        let si_cols = strategy.si_cols();
        let mut update_count = false;
        for field in fields.iter() {
            if field.0.equal_ignore_case(si_cols[2].as_bytes()) {
                update_count = true;
                if field.1.start_with(0, "-".as_bytes()) {
                    let count = field.1.str_num(1..);
                    let _ = write!(
                        f,
                        "{}=greatest(0,cast({} as signed) - {})",
                        Key(&field.0),
                        Key(&field.0),
                        count
                    );
                } else {
                    let count = field.1.str_num(..);
                    let _ = write!(
                        f,
                        "{}=greatest(0,cast({} as signed) + {})",
                        Key(&field.0),
                        Key(&field.0),
                        count,
                    );
                }
            }
        }
        for (i, field) in fields.iter().enumerate() {
            if !field.0.equal_ignore_case(si_cols[2].as_bytes()) {
                if i == 0 && !update_count {
                    let _ = write!(f, "{}={}", Key(&field.0), Val(&field.1));
                } else {
                    let _ = write!(f, ",{}={}", Key(&field.0), Val(&field.1));
                }
            }
        }
        Ok(())
    }
}

struct SiKeysAndUpdateOrDelConds<'a, S>(&'a S, &'a VectorCmd, &'a NaiveDate);
impl<'a, S: Strategy> Display for SiKeysAndUpdateOrDelConds<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(
            strategy,
            VectorCmd {
                route,
                keys,
                wheres,
                ..
            },
            date,
        ) = self;
        for (i, key) in (&mut strategy.keys_with_type()).enumerate() {
            if let KeysType::Keys(key) = key {
                if i == 0 {
                    let _ = write!(f, "`{}`={}", key, Val(&keys[i]));
                } else {
                    let _ = write!(f, " and `{}`={}", key, Val(&keys[i]));
                }
                break;
            }
        }
        let mut has_count_type = false;
        let si_cols = strategy.si_cols();
        for w in wheres {
            let mut added = false;
            //条件中和si相同的列写入条件
            for col in si_cols {
                if w.field.equal(col.as_bytes()) {
                    has_count_type = true;
                    let _ = write!(f, " and {}", ConditionDisplay(w));
                    added = true;
                    break;
                }
            }
            // 聚合查询会带入timeline的额外条件，不需要带入sql;只在si查询才需要增加这些条件  fishermen
            if !route.expect("aggregation").is_aggregation() && !added {
                let _ = write!(f, " and {}", ConditionDisplay(w));
            }
        }
        log::info!("+++ ==== si cols {} / {}", si_cols.len(), has_count_type);
        //必须提供count_type
        if si_cols.len() > 2 && !has_count_type {
            log::debug!("++++ don't has count type");
            return Err(std::fmt::Error);
        }
        //date
        let _ = write!(
            f,
            " and {}='{}-{}-{}'",
            si_cols.first().unwrap(),
            date.year(),
            date.month(),
            date.day(),
        );
        Ok(())
    }
}
