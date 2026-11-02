use crate::invalid;
use rusqlite::Result;
use serde::Deserialize;
use serde_json::Value;

pub const MAX_SECONDS: i64 = 31_536_000;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub priority: i32,
    pub max_attempts: i64,
    pub dedup_key: Option<String>,
    pub metadata: Value,
    pub retry_base_seconds: i64,
    pub retry_max_seconds: i64,
    pub ttl_seconds: Option<i64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            priority: 0,
            max_attempts: 5,
            dedup_key: None,
            metadata: serde_json::json!({}),
            retry_base_seconds: 0,
            retry_max_seconds: 3600,
            ttl_seconds: None,
        }
    }
}

impl Options {
    pub fn parse(text: &str) -> Result<Self> {
        let shape: Value = serde_json::from_str(text).map_err(|e| invalid(&e.to_string()))?;
        if !shape.is_object() {
            return Err(invalid("options must be a JSON object"));
        }
        let value: Self = serde_json::from_str(text).map_err(|e| invalid(&e.to_string()))?;
        value.validate()
    }

    pub fn validate(self) -> Result<Self> {
        if !(1..=1_000_000).contains(&self.max_attempts) {
            return Err(invalid("max_attempts must be 1..1000000"));
        }
        if !(0..=MAX_SECONDS).contains(&self.retry_base_seconds)
            || !(self.retry_base_seconds..=MAX_SECONDS).contains(&self.retry_max_seconds)
        {
            return Err(invalid(
                "retry seconds must satisfy 0 <= base <= max <= 31536000",
            ));
        }
        if self
            .ttl_seconds
            .is_some_and(|v| !(1..=MAX_SECONDS).contains(&v))
        {
            return Err(invalid("ttl_seconds must be 1..31536000"));
        }
        if self.dedup_key.as_ref().is_some_and(|v| v.is_empty()) {
            return Err(invalid("dedup_key must not be empty"));
        }
        Ok(self)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchJob {
    pub payload: String,
    #[serde(default)]
    pub delay_seconds: i64,
    #[serde(default)]
    pub options: Options,
}

impl BatchJob {
    pub fn parse_many(text: &str) -> Result<Vec<Self>> {
        let shape: Value = serde_json::from_str(text).map_err(|e| invalid(&e.to_string()))?;
        let array = shape
            .as_array()
            .ok_or_else(|| invalid("batch must be a JSON array"))?;
        if array.len() > 1000 {
            return Err(invalid("batch must contain at most 1000 jobs"));
        }
        for job in array {
            if !job.is_object() || job.get("options").is_some_and(|v| !v.is_object()) {
                return Err(invalid("batch entries and options must be JSON objects"));
            }
        }
        serde_json::from_str(text).map_err(|e| invalid(&e.to_string()))
    }
}
