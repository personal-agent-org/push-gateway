use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const OAUTH_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Delivered,
    Unregistered,
    UpstreamError,
}

#[derive(Clone, Deserialize)]
pub struct Credentials {
    project_id: String,
    client_email: String,
    private_key: String,
    token_uri: String,
}

pub fn load_credentials(path: &str) -> Option<Credentials> {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .or_else(|| {
            tracing::warn!("FCM credentials unreadable");
            None
        })
}

struct CachedToken {
    value: String,
    expires_at: u64,
}
pub struct FcmClient {
    creds: Credentials,
    http: reqwest::Client,
    token: Mutex<Option<CachedToken>>,
    endpoint: String,
}

impl FcmClient {
    pub fn new(creds: Credentials) -> Result<Self, reqwest::Error> {
        let endpoint = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            creds.project_id
        );
        Ok(Self {
            creds,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()?,
            token: Mutex::new(None),
            endpoint,
        })
    }
    #[cfg(test)]
    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }
    pub async fn send(
        &self,
        push_token: &str,
        frame_json: &str,
        priority: &str,
        ttl: Option<i64>,
        collapse_key: Option<&str>,
    ) -> SendOutcome {
        match self
            .try_send(push_token, frame_json, priority, ttl, collapse_key)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error=%e, "FCM request failed");
                SendOutcome::UpstreamError
            }
        }
    }
    async fn try_send(
        &self,
        push_token: &str,
        frame_json: &str,
        priority: &str,
        ttl: Option<i64>,
        collapse_key: Option<&str>,
    ) -> Result<SendOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let mut android = serde_json::Map::new();
        android.insert("priority".into(), json!(priority));
        if let Some(ttl) = ttl {
            android.insert("ttl".into(), json!(format!("{ttl}s")));
        }
        if let Some(key) = collapse_key {
            android.insert("collapse_key".into(), json!(key));
        }
        let message =
            json!({"message":{"token":push_token,"data":{"frame":frame_json},"android":android}});
        let token = self.access_token().await?;
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(token)
            .json(&message)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(SendOutcome::Delivered);
        }
        if status.as_u16() == 404 || status.as_u16() == 410 {
            return Ok(SendOutcome::Unregistered);
        }
        let body = response.bytes().await?;
        if status.as_u16() == 400 && is_unregistered(&body) {
            return Ok(SendOutcome::Unregistered);
        }
        tracing::warn!(status=%status, "FCM send failed");
        Ok(SendOutcome::UpstreamError)
    }
    async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut cached = self.token.lock().await;
        let now = epoch();
        if let Some(token) = cached
            .as_ref()
            .filter(|t| now < t.expires_at.saturating_sub(300))
        {
            return Ok(token.value.clone());
        }
        #[derive(Serialize)]
        struct Claims<'a> {
            iss: &'a str,
            scope: &'a str,
            aud: &'a str,
            iat: u64,
            exp: u64,
        }
        let assertion = encode(
            &Header::new(Algorithm::RS256),
            &Claims {
                iss: &self.creds.client_email,
                scope: OAUTH_SCOPE,
                aud: &self.creds.token_uri,
                iat: now,
                exp: now + 3600,
            },
            &EncodingKey::from_rsa_pem(self.creds.private_key.as_bytes())?,
        )?;
        let response = self
            .http
            .post(&self.creds.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?;
        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
            #[serde(default = "default_expiry")]
            expires_in: u64,
        }
        let token: TokenResponse = response.json().await?;
        let value = token.access_token.clone();
        *cached = Some(CachedToken {
            value: token.access_token,
            expires_at: epoch() + token.expires_in,
        });
        Ok(value)
    }
}
fn default_expiry() -> u64 {
    3600
}
fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn is_unregistered(raw: &[u8]) -> bool {
    serde_json::from_slice::<Value>(raw)
        .ok()
        .and_then(|v| {
            v.pointer("/error/details")
                .and_then(Value::as_array)
                .cloned()
        })
        .is_some_and(|details| {
            details
                .iter()
                .any(|d| d.get("errorCode").and_then(Value::as_str) == Some("UNREGISTERED"))
        })
}
