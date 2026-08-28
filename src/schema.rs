use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Map, Number, Value};

use crate::config::MAX_DATA_BYTES;

static TYPE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-z_]{1,64}$").unwrap());
static PAYLOAD_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z0-9_:-]{1,128}$").unwrap());

#[derive(Debug, Clone)]
pub struct PushRequest {
    pub push_token: String,
    pub priority: String,
    pub ttl: Option<i64>,
    pub collapse_id: Option<String>,
    pub data_type: String,
    pub frame_json: String,
}

impl PushRequest {
    #[allow(clippy::result_unit_err)]
    pub fn parse(raw: &[u8]) -> Result<Self, ()> {
        let Value::Object(mut root) = serde_json::from_slice(raw).map_err(|_| ())? else {
            return Err(());
        };
        if root.keys().any(|k| {
            !matches!(
                k.as_str(),
                "v" | "push_token"
                    | "platform"
                    | "app_id"
                    | "priority"
                    | "ttl"
                    | "collapse_id"
                    | "data"
            )
        }) {
            return Err(());
        }
        if take_int(&mut root, "v", false)? != Some(1) {
            return Err(());
        }
        let push_token = take_string(&mut root, "push_token", false)?.ok_or(())?;
        if !(8..=4096).contains(&push_token.chars().count()) {
            return Err(());
        }
        if take_string(&mut root, "platform", false)?.as_deref() != Some("android") {
            return Err(());
        }
        let priority = take_string(&mut root, "priority", false)?.ok_or(())?;
        if priority != "high" && priority != "normal" {
            return Err(());
        }
        let app_id = take_string(&mut root, "app_id", true)?;
        if app_id.as_ref().is_some_and(|s| s.chars().count() > 255) {
            return Err(());
        }
        let ttl = take_int(&mut root, "ttl", true)?;
        if ttl.is_some_and(|v| !(0..=86400).contains(&v)) {
            return Err(());
        }
        let collapse_id = take_string(&mut root, "collapse_id", true)?;
        if collapse_id.as_ref().is_some_and(|s| s.chars().count() > 64) {
            return Err(());
        }
        let Value::Object(mut data) = root.remove("data").ok_or(())? else {
            return Err(());
        };
        let data_type = data
            .get("type")
            .and_then(Value::as_str)
            .ok_or(())?
            .to_owned();
        let payload_id = data
            .get("payload_id")
            .and_then(Value::as_str)
            .ok_or(())?
            .to_owned();
        if !TYPE_RE.is_match(&data_type) || !PAYLOAD_RE.is_match(&payload_id) {
            return Err(());
        }
        if let Some(v) = data.get_mut("v") {
            *v = Value::Number(Number::from(value_to_int(v).ok_or(())?));
        }
        for (key, value) in &data {
            if matches!(key.as_str(), "type" | "payload_id" | "v") {
                continue;
            }
            let scalar = match value {
                Value::String(_) => true,
                Value::Number(n) => n.is_i64() || n.is_u64(),
                _ => false,
            };
            if !scalar {
                return Err(());
            }
        }
        // Pydantic model_dump emits declared fields first, then extras.
        let mut normalized = Map::new();
        normalized.insert("type".into(), Value::String(data_type.clone()));
        normalized.insert("payload_id".into(), Value::String(payload_id));
        if let Some(v) = data.remove("v")
            && !v.is_null()
        {
            normalized.insert("v".into(), v);
        }
        for (k, v) in data {
            if !matches!(k.as_str(), "type" | "payload_id") {
                normalized.insert(k, v);
            }
        }
        let frame_json = serde_json::to_string(&normalized).map_err(|_| ())?;
        if frame_json.len() > MAX_DATA_BYTES {
            return Err(());
        }
        Ok(Self {
            push_token,
            priority,
            ttl,
            collapse_id,
            data_type,
            frame_json,
        })
    }
}

fn take_string(
    map: &mut Map<String, Value>,
    key: &str,
    nullable: bool,
) -> Result<Option<String>, ()> {
    match map.remove(key) {
        None => Ok(None),
        Some(Value::Null) if nullable => Ok(None),
        Some(Value::String(v)) => Ok(Some(v)),
        _ => Err(()),
    }
}

fn value_to_int(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().filter(|n| n.fract() == 0.0).map(|n| n as i64)),
        Value::String(s) => s.parse().ok(),
        Value::Bool(v) => Some(i64::from(*v)),
        _ => None,
    }
}

fn take_int(map: &mut Map<String, Value>, key: &str, nullable: bool) -> Result<Option<i64>, ()> {
    match map.remove(key) {
        None => Ok(None),
        Some(Value::Null) if nullable => Ok(None),
        Some(v) => value_to_int(&v).map(Some).ok_or(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    #[test]
    fn validates_and_normalizes_frame() {
        let req = PushRequest::parse(br#"{"v":1,"push_token":"12345678","platform":"android","priority":"high","data":{"blob":2,"payload_id":"x","type":"chat_reply","v":"2"}}"#).unwrap();
        assert_eq!(
            req.frame_json,
            r#"{"type":"chat_reply","payload_id":"x","v":2,"blob":2}"#
        );
    }

    #[derive(Deserialize)]
    struct GoldenCase {
        name: String,
        valid: bool,
        body: Value,
    }

    #[test]
    fn matches_language_neutral_golden_cases() {
        let cases: Vec<GoldenCase> =
            serde_json::from_str(include_str!("../tests/golden/schema.json")).unwrap();
        for case in cases {
            let actual = PushRequest::parse(&serde_json::to_vec(&case.body).unwrap()).is_ok();
            assert_eq!(actual, case.valid, "{}", case.name);
        }
    }
}
