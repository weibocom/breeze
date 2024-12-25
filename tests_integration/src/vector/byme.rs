use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct LikeByMe {
    pub(crate) uid: i64,
    pub(crate) like_id: i64,
    pub(crate) object_id: i64,
    pub(crate) object_type: i64,
}

#[derive(Debug, Serialize, Deserialize)]

pub(crate) struct LikeByMeSi {
    pub(crate) uid: i64,
    pub(crate) object_type: i64,
    pub(crate) start_date: String,
    pub(crate) count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LikeByMeResponse {
    pub(crate) header: LikeByMeMeta,
    pub(crate) body: Vec<LikeByMe>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct LikeByMeMeta {
    like_id: String,
    object_id: String,
    object_type: String,
}

// bulk(bulk(status("object_type"), status("count")), bulk(int(1), int(17))))
#[derive(Debug, Default, Serialize, Deserialize)]

pub(crate) struct LikeByMeVcardResponse {
    pub(crate) header: LikeByMeVcardMeta,
    pub(crate) body: LikeByMeVcard,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct LikeByMeVcard {
    pub(crate) object_type: i64,
    pub(crate) count: i64,
}
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct LikeByMeSiMeta {
    pub(crate) uid: String,
    pub(crate) object_type: String,
    pub(crate) start_date: String,
    pub(crate) count: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct LikeByMeVcardMeta {
    pub(crate) object_type: String,
    pub(crate) count: String,
}

impl Display for LikeByMeResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LikeByMeResponse {{ header: {:?}, body: {:?} }}",
            self.header, self.body
        )
    }
}

impl Display for LikeByMeMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LikeByMeMeta {{ like_id: {}, object_id: {}, object_type: {} }}",
            self.like_id, self.object_id, self.object_type
        )
    }
}

impl Display for LikeByMe {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LikeByMeResponseBody {{ like_id: {}, object_id: {}, object_type: {} }}",
            self.like_id, self.object_id, self.object_type
        )
    }
}

impl Display for LikeByMeVcardResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LikeByMeSiResponse {{ header: {:?}, body: {:?} }}",
            self.header, self.body
        )
    }
}
impl Display for LikeByMeSiMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LikeByMeSiMeta {{ uid: {}, object_type: {}, start_date: {}, count: {} }}",
            self.uid, self.object_type, self.start_date, self.count
        )
    }
}
impl Display for LikeByMeSi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LikeByMeSi {{ uid: {}, object_type: {}, start_date: {}, count: {} }}",
            self.uid, self.object_type, self.start_date, self.count
        )
    }
}
