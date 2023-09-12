#![cfg(feature = "regex")]

use lexical::parse;
use regex::bytes::Regex;
use std::time::Duration;

pub fn parse_duration(bytes: &[u8]) -> Option<Duration> {
    if let Some((is_neg, hours, minutes, seconds, micros)) = parse_mysql_time_string(bytes) {
        if !is_neg {
            return Some(Duration::new(
                (hours * 3600 + minutes * 60 + seconds) as u64,
                micros as u32 * 1000,
            ));
        }
    }
    None
}

lazy_static::lazy_static! {
    static ref DATETIME_RE_YMD: Regex = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    static ref DATETIME_RE_YMD_HMS: Regex =
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$").unwrap();
    static ref DATETIME_RE_YMD_HMS_NS: Regex =
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap();
    static ref TIME_RE_HH_MM_SS: Regex = Regex::new(r"^\d{2}:[0-5]\d:[0-5]\d$").unwrap();
    static ref TIME_RE_HH_MM_SS_MS: Regex =
        Regex::new(r"^\d{2}:[0-5]\d:[0-5]\d\.\d{1,6}$").unwrap();
    static ref TIME_RE_HHH_MM_SS: Regex = Regex::new(r"^[0-8]\d\d:[0-5]\d:[0-5]\d$").unwrap();
    static ref TIME_RE_HHH_MM_SS_MS: Regex =
        Regex::new(r"^[0-8]\d\d:[0-5]\d:[0-5]\d\.\d{1,6}$").unwrap();
}

/// Returns (is_neg, hours, minutes, seconds, microseconds)
pub fn parse_mysql_time_string(mut bytes: &[u8]) -> Option<(bool, u32, u32, u32, u32)> {
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    #[repr(u8)]
    enum TimeKind {
        HhMmSs = 0,
        HhhMmSs,
        HhMmSsMs,
        HhhMmSsMs,
    }

    if bytes.len() < 8 {
        return None;
    }

    let is_neg = bytes[0] == b'-';
    if is_neg {
        bytes = &bytes[1..];
    }

    let len = bytes.len();

    let kind = if len == 8 && TIME_RE_HH_MM_SS.is_match(bytes) {
        TimeKind::HhMmSs
    } else if len == 9 && TIME_RE_HHH_MM_SS.is_match(bytes) {
        TimeKind::HhhMmSs
    } else if TIME_RE_HH_MM_SS_MS.is_match(bytes) {
        TimeKind::HhMmSsMs
    } else if TIME_RE_HHH_MM_SS_MS.is_match(bytes) {
        TimeKind::HhhMmSsMs
    } else {
        return None;
    };

    let (hour_pos, min_pos, sec_pos, micros_pos) = match kind {
        TimeKind::HhMmSs => (..2, 3..5, 6..8, None),
        TimeKind::HhMmSsMs => (..2, 3..5, 6..8, Some(9..)),
        TimeKind::HhhMmSs => (..3, 4..6, 7..9, None),
        TimeKind::HhhMmSsMs => (..3, 4..6, 7..9, Some(10..)),
    };

    Some((
        is_neg,
        parse(&bytes[hour_pos]).unwrap(),
        parse(&bytes[min_pos]).unwrap(),
        parse(&bytes[sec_pos]).unwrap(),
        micros_pos.map(|pos| parse_micros(&bytes[pos])).unwrap_or(0),
    ))
}

/// Returns (year, month, day, hour, minute, second, micros)
#[cfg(any(feature = "chrono", all(feature = "time", test)))]
fn parse_mysql_datetime_string(bytes: &[u8]) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    let len = bytes.len();

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    #[repr(u8)]
    enum DateTimeKind {
        Ymd = 0,
        YmdHms,
        YmdHmsMs,
    }

    let kind = if len == 10 && DATETIME_RE_YMD.is_match(bytes) {
        DateTimeKind::Ymd
    } else if len == 19 && DATETIME_RE_YMD_HMS.is_match(bytes) {
        DateTimeKind::YmdHms
    } else if 20 < len && len < 27 && DATETIME_RE_YMD_HMS_NS.is_match(bytes) {
        DateTimeKind::YmdHmsMs
    } else {
        return None;
    };

    let (year, month, day, hour, minute, second, micros) = match kind {
        DateTimeKind::Ymd => (..4, 5..7, 8..10, None, None, None, None),
        DateTimeKind::YmdHms => (
            ..4,
            5..7,
            8..10,
            Some(11..13),
            Some(14..16),
            Some(17..19),
            None,
        ),
        DateTimeKind::YmdHmsMs => (
            ..4,
            5..7,
            8..10,
            Some(11..13),
            Some(14..16),
            Some(17..19),
            Some(20..),
        ),
    };

    Some((
        parse(&bytes[year]).unwrap(),
        parse(&bytes[month]).unwrap(),
        parse(&bytes[day]).unwrap(),
        hour.map(|pos| parse(&bytes[pos]).unwrap()).unwrap_or(0),
        minute.map(|pos| parse(&bytes[pos]).unwrap()).unwrap_or(0),
        second.map(|pos| parse(&bytes[pos]).unwrap()).unwrap_or(0),
        micros.map(|pos| parse_micros(&bytes[pos])).unwrap_or(0),
    ))
}

pub fn parse_micros(micros_bytes: &[u8]) -> u32 {
    let mut micros = parse(micros_bytes).unwrap();

    let mut pad_zero_cnt = 0;
    for b in micros_bytes.iter() {
        if *b == b'0' {
            pad_zero_cnt += 1;
        } else {
            break;
        }
    }

    for _ in 0..(6 - pad_zero_cnt - (micros_bytes.len() - pad_zero_cnt)) {
        micros *= 10;
    }
    micros
}
