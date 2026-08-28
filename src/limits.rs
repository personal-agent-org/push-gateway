use async_trait::async_trait;
use chrono::{DateTime, Days, Utc};
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

use crate::{
    config::{BURST_PER_MINUTE, IP_PER_MINUTE, TOMBSTONE_TTL_SECONDS},
    metrics::Metrics,
};

#[derive(Debug, Error)]
#[error("rate-limit store unavailable")]
pub struct StoreUnavailable;

#[async_trait]
pub trait Store: Send + Sync {
    async fn incr(&self, key: &str, ttl: u64) -> Result<i64, String>;
    async fn get_int(&self, key: &str) -> Result<i64, String>;
    async fn set_flag(&self, key: &str, ttl: u64) -> Result<(), String>;
    async fn has_flag(&self, key: &str) -> Result<bool, String>;
}

#[derive(Default)]
pub struct MemoryStore {
    data: Mutex<HashMap<String, (i64, Instant)>>,
}

#[async_trait]
impl Store for MemoryStore {
    async fn incr(&self, key: &str, ttl: u64) -> Result<i64, String> {
        let mut data = self.data.lock().map_err(|e| e.to_string())?;
        let now = Instant::now();
        let entry = data
            .entry(key.into())
            .or_insert((0, now + Duration::from_secs(ttl)));
        if entry.1 <= now {
            *entry = (0, now + Duration::from_secs(ttl));
        }
        entry.0 += 1;
        Ok(entry.0)
    }
    async fn get_int(&self, key: &str) -> Result<i64, String> {
        let mut data = self.data.lock().map_err(|e| e.to_string())?;
        Ok(match data.get(key) {
            Some((v, exp)) if *exp > Instant::now() => *v,
            Some(_) => {
                data.remove(key);
                0
            }
            None => 0,
        })
    }
    async fn set_flag(&self, key: &str, ttl: u64) -> Result<(), String> {
        self.data
            .lock()
            .map_err(|e| e.to_string())?
            .insert(key.into(), (1, Instant::now() + Duration::from_secs(ttl)));
        Ok(())
    }
    async fn has_flag(&self, key: &str) -> Result<bool, String> {
        Ok(self.get_int(key).await? != 0)
    }
}

pub struct RedisStore {
    client: redis::Client,
}
impl RedisStore {
    pub fn new(url: &str) -> Result<Self, redis::RedisError> {
        Ok(Self {
            client: redis::Client::open(url)?,
        })
    }
}

#[async_trait]
impl Store for RedisStore {
    async fn incr(&self, key: &str, ttl: u64) -> Result<i64, String> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        // One server-side transaction, preserving an existing TTL.
        let (count, _): (i64, i64) = redis::pipe()
            .atomic()
            .cmd("INCR")
            .arg(key)
            .cmd("EXPIRE")
            .arg(key)
            .arg(ttl)
            .arg("NX")
            .query_async(&mut con)
            .await
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    async fn get_int(&self, key: &str) -> Result<i64, String> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        con.get::<_, Option<i64>>(key)
            .await
            .map(|v| v.unwrap_or(0))
            .map_err(|e| e.to_string())
    }
    async fn set_flag(&self, key: &str, ttl: u64) -> Result<(), String> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        con.set_ex::<_, _, ()>(key, "1", ttl)
            .await
            .map_err(|e| e.to_string())
    }
    async fn has_flag(&self, key: &str) -> Result<bool, String> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        con.exists(key).await.map_err(|e| e.to_string())
    }
}

#[derive(Clone, Copy)]
pub struct DailyCounts {
    pub successful: i64,
    pub errors: i64,
}

pub struct RateLimiter {
    store: Arc<dyn Store>,
    global: i64,
    metrics: Arc<Metrics>,
}
impl RateLimiter {
    pub fn new(store: Arc<dyn Store>, global: i64, metrics: Arc<Metrics>) -> Self {
        Self {
            store,
            global,
            metrics,
        }
    }
    async fn incr(&self, key: &str, ttl: u64) -> Result<i64, StoreUnavailable> {
        self.store.incr(key, ttl).await.map_err(|e| {
            tracing::warn!(error=%e, "store incr failed, refusing");
            self.metrics.store_failed("incr");
            StoreUnavailable
        })
    }
    async fn get(&self, key: &str) -> Result<i64, StoreUnavailable> {
        self.store.get_int(key).await.map_err(|e| {
            tracing::warn!(error=%e, "store get failed, refusing");
            self.metrics.store_failed("get");
            StoreUnavailable
        })
    }
    async fn has(&self, key: &str) -> Result<bool, StoreUnavailable> {
        self.store.has_flag(key).await.map_err(|e| {
            tracing::warn!(error=%e, "store check failed, refusing");
            self.metrics.store_failed("has");
            StoreUnavailable
        })
    }
    pub async fn ip_allowed(&self, ip: &str) -> Result<bool, StoreUnavailable> {
        Ok(self.incr(&format!("ip:{ip}:{}", minute()), 120).await? <= IP_PER_MINUTE)
    }
    pub async fn is_tombstoned(&self, th: &str) -> Result<bool, StoreUnavailable> {
        self.has(&format!("tomb:{th}")).await
    }
    pub async fn burst_allowed(&self, th: &str) -> Result<bool, StoreUnavailable> {
        Ok(self.incr(&format!("burst:{th}:{}", minute()), 120).await? <= BURST_PER_MINUTE)
    }
    pub async fn global_allowed(&self) -> Result<bool, StoreUnavailable> {
        Ok(self.incr(&format!("global:{}", minute()), 120).await? <= self.global)
    }
    pub async fn daily_counts(&self, th: &str) -> Result<DailyCounts, StoreUnavailable> {
        let day = utc_day();
        Ok(DailyCounts {
            successful: self.get(&format!("day_ok:{th}:{day}")).await?,
            errors: self.get(&format!("day_err:{th}:{day}")).await?,
        })
    }
    pub async fn record_result(&self, th: &str, success: bool) {
        let kind = if success { "day_ok" } else { "day_err" };
        if let Err(e) = self
            .store
            .incr(
                &format!("{kind}:{th}:{}", utc_day()),
                seconds_to_utc_midnight() + 3600,
            )
            .await
        {
            tracing::warn!(error=%e, "store incr failed, ignoring");
            self.metrics.store_failed("incr");
        }
    }
    pub async fn tombstone(&self, th: &str) {
        if let Err(e) = self
            .store
            .set_flag(&format!("tomb:{th}"), TOMBSTONE_TTL_SECONDS)
            .await
        {
            tracing::warn!(error=%e, "store set failed, ignoring");
            self.metrics.store_failed("set");
        }
    }
}

fn minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60
}
pub fn token_hash(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}
pub fn utc_day() -> String {
    Utc::now().format("%Y%m%d").to_string()
}
pub fn next_utc_midnight() -> DateTime<Utc> {
    let tomorrow = Utc::now()
        .date_naive()
        .checked_add_days(Days::new(1))
        .unwrap();
    tomorrow.and_hms_opt(0, 0, 0).unwrap().and_utc()
}
pub fn seconds_to_utc_midnight() -> u64 {
    (next_utc_midnight() - Utc::now()).num_seconds().max(1) as u64
}
pub fn utc_midnight_iso() -> String {
    next_utc_midnight().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_is_stable() {
        assert_eq!(
            token_hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
