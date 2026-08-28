use std::env;

pub const MAX_BODY_BYTES: usize = 4096;
pub const MAX_DATA_BYTES: usize = 1024;
pub const DAILY_QUOTA: i64 = 500;
pub const BURST_PER_MINUTE: i64 = 30;
pub const IP_PER_MINUTE: i64 = 600;
pub const TOMBSTONE_TTL_SECONDS: u64 = 7 * 24 * 3600;

#[derive(Clone, Debug)]
pub struct Settings {
    pub fcm_credentials_file: String,
    pub redis_url: String,
    pub trust_proxy: bool,
    pub global_per_minute: i64,
    pub port: u16,
}

impl Settings {
    pub fn from_env() -> Self {
        Self {
            fcm_credentials_file: env::var("FCM_CREDENTIALS_FILE").unwrap_or_default(),
            redis_url: env::var("REDIS_URL").unwrap_or_default(),
            trust_proxy: env_bool("TRUST_PROXY"),
            global_per_minute: env::var("GLOBAL_PER_MINUTE")
                .unwrap_or_else(|_| "50000".into())
                .parse()
                .expect("GLOBAL_PER_MINUTE must be an integer"),
            port: env::var("PORT")
                .unwrap_or_else(|_| "8080".into())
                .parse()
                .expect("PORT must be an integer"),
        }
    }
}

fn env_bool(name: &str) -> bool {
    env::var(name).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}
