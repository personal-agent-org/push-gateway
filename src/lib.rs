pub mod config;
pub mod fcm;
pub mod limits;
pub mod metrics;
pub mod schema;

use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, State},
    http::{HeaderMap, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Instant};

use config::{DAILY_QUOTA, MAX_BODY_BYTES, Settings};
use fcm::{FcmClient, SendOutcome, load_credentials};
use limits::{
    DailyCounts, MemoryStore, RateLimiter, RedisStore, Store, seconds_to_utc_midnight, token_hash,
    utc_midnight_iso,
};
use metrics::Metrics;
use schema::PushRequest;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub struct AppState {
    settings: Settings,
    fcm: Option<FcmClient>,
    limiter: RateLimiter,
    metrics: Arc<Metrics>,
}

pub fn app(settings: Settings) -> Router {
    let backend = if settings.redis_url.is_empty() {
        "memory"
    } else {
        "redis"
    };
    let creds = if settings.fcm_credentials_file.is_empty() {
        None
    } else {
        load_credentials(&settings.fcm_credentials_file)
    };
    let fcm = creds.and_then(|c| {
        FcmClient::new(c)
            .map_err(|e| tracing::warn!(error=%e, "FCM client initialization failed"))
            .ok()
    });
    let store: Arc<dyn Store> = if settings.redis_url.is_empty() {
        Arc::new(MemoryStore::default())
    } else {
        Arc::new(RedisStore::new(&settings.redis_url).expect("invalid REDIS_URL"))
    };
    app_with(settings, fcm, store, backend)
}

pub fn app_with(
    settings: Settings,
    fcm: Option<FcmClient>,
    store: Arc<dyn Store>,
    backend: &str,
) -> Router {
    let metrics = Arc::new(Metrics::new(VERSION, fcm.is_some(), backend));
    let limiter = RateLimiter::new(store, settings.global_per_minute, metrics.clone());
    let state = Arc::new(AppState {
        settings,
        fcm,
        limiter,
        metrics,
    });
    Router::new()
        .route("/api/v1/push", post(push))
        .route("/api/v1/info", get(info))
        .route("/healthz", get(health))
        .route("/metrics", get(metrics_endpoint))
        .with_state(state)
}

#[axum::debug_handler]
async fn push(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response {
    match push_inner(&state, Some(peer), request).await {
        Ok(r) => r,
        Err(()) => {
            state.metrics.reject("store_unavailable");
            response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error":"unavailable"}),
                Some(("Retry-After", "30")),
            )
        }
    }
}

async fn push_inner(
    state: &AppState,
    peer: Option<SocketAddr>,
    request: Request<Body>,
) -> Result<Response, ()> {
    let started = Instant::now();
    let ip = client_ip(request.headers(), state.settings.trust_proxy, peer);
    if !state.limiter.ip_allowed(&ip).await.map_err(|_| ())? {
        state.metrics.reject("ip_rate_limit");
        return Ok(response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"rate_limited"}),
            Some(("Retry-After", "60")),
        ));
    }
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > MAX_BODY_BYTES)
    {
        state.metrics.reject("too_large");
        return Ok(response(
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({"error":"too_large"}),
            None,
        ));
    }
    let mut stream = request.into_body().into_data_stream();
    let mut raw = Vec::new();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            state.metrics.reject("invalid");
            return Ok(response(
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid"}),
                None,
            ));
        };
        if raw.len() + chunk.len() > MAX_BODY_BYTES {
            state.metrics.reject("too_large");
            return Ok(response(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error":"too_large"}),
                None,
            ));
        }
        raw.extend_from_slice(&chunk);
    }
    let Ok(req) = PushRequest::parse(&raw) else {
        state.metrics.reject("invalid");
        return Ok(response(
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid"}),
            None,
        ));
    };
    let Some(fcm) = state.fcm.as_ref() else {
        state.metrics.reject("not_configured");
        return Ok(response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"not_configured"}),
            None,
        ));
    };
    let th = token_hash(&req.push_token);
    if state.limiter.is_tombstoned(&th).await.map_err(|_| ())? {
        state.limiter.record_result(&th, false).await;
        state.metrics.reject("tombstoned");
        log_send("tombstoned", &th, &req, started);
        return Ok(response(
            StatusCode::NOT_FOUND,
            json!({"error":"unregistered","rateLimits":limits(state,&th).await?}),
            None,
        ));
    }
    if !state.limiter.burst_allowed(&th).await.map_err(|_| ())? {
        state.metrics.reject("burst_rate_limit");
        return Ok(response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"rate_limited","rateLimits":limits(state,&th).await?}),
            Some(("Retry-After", "60")),
        ));
    }
    let counts = state.limiter.daily_counts(&th).await.map_err(|_| ())?;
    if counts.successful >= DAILY_QUOTA {
        state.metrics.reject("daily_quota");
        return Ok(response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"rate_limited","rateLimits":limits_body(counts)}),
            Some(("Retry-After", &seconds_to_utc_midnight().to_string())),
        ));
    }
    if !state.limiter.global_allowed().await.map_err(|_| ())? {
        state.metrics.reject("global_rate_limit");
        return Ok(response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"rate_limited"}),
            Some(("Retry-After", "60")),
        ));
    }
    let upstream = Instant::now();
    let outcome = fcm
        .send(
            &req.push_token,
            &req.frame_json,
            &req.priority,
            req.ttl,
            req.collapse_id.as_deref(),
        )
        .await;
    let seconds = upstream.elapsed().as_secs_f64();
    match outcome {
        SendOutcome::Delivered => {
            state.limiter.record_result(&th, true).await;
            state.metrics.sent("delivered", &req.priority, seconds);
            log_send("delivered", &th, &req, started);
            Ok(response(
                StatusCode::OK,
                json!({"rateLimits":limits(state,&th).await?}),
                None,
            ))
        }
        SendOutcome::Unregistered => {
            state.limiter.record_result(&th, false).await;
            state.limiter.tombstone(&th).await;
            state.metrics.sent("unregistered", &req.priority, seconds);
            log_send("unregistered", &th, &req, started);
            Ok(response(
                StatusCode::NOT_FOUND,
                json!({"error":"unregistered","rateLimits":limits(state,&th).await?}),
                None,
            ))
        }
        SendOutcome::UpstreamError => {
            state.limiter.record_result(&th, false).await;
            state.metrics.sent("upstream_error", &req.priority, seconds);
            log_send("upstream_error", &th, &req, started);
            Ok(response(
                StatusCode::BAD_GATEWAY,
                json!({"error":"upstream"}),
                None,
            ))
        }
    }
}

async fn limits(state: &AppState, th: &str) -> Result<Value, ()> {
    Ok(limits_body(
        state.limiter.daily_counts(th).await.map_err(|_| ())?,
    ))
}
fn limits_body(c: DailyCounts) -> Value {
    json!({"successful":c.successful,"errors":c.errors,"maximum":DAILY_QUOTA,"remaining":0.max(DAILY_QUOTA-c.successful),"resetsAt":utc_midnight_iso()})
}
fn response(status: StatusCode, body: Value, extra: Option<(&str, &str)>) -> Response {
    let mut r = (status, axum::Json(body)).into_response();
    if let Some((k, v)) = extra {
        r.headers_mut().insert(
            header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            header::HeaderValue::from_str(v).unwrap(),
        );
    }
    r
}
fn client_ip(headers: &HeaderMap, trust: bool, peer: Option<SocketAddr>) -> String {
    if trust {
        for name in ["cf-connecting-ip", "x-real-ip"] {
            if let Some(v) = headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                return v.into();
            }
        }
        if let Some(v) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit(',').next())
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return v.into();
        }
    }
    peer.map(|p| p.ip().to_string())
        .unwrap_or_else(|| "unknown".into())
}
async fn info() -> impl IntoResponse {
    axum::Json(
        json!({"name":"personal-agent-push-gateway","version":VERSION,"platforms":["android"],"max_body_bytes":MAX_BODY_BYTES,"daily_quota":DAILY_QUOTA}),
    )
}
async fn health() -> impl IntoResponse {
    axum::Json(json!({"ok":true}))
}
async fn metrics_endpoint(State(s): State<Arc<AppState>>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        s.metrics.render(),
    )
        .into_response()
}
fn log_send(outcome: &str, th: &str, req: &PushRequest, started: Instant) {
    tracing::info!(outcome,token=%&th[..8],frame_type=%req.data_type,priority=%req.priority,latency_ms=started.elapsed().as_millis(),"send");
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use axum::http::{Method, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn settings() -> Settings {
        Settings {
            fcm_credentials_file: String::new(),
            redis_url: String::new(),
            trust_proxy: false,
            global_per_minute: 50_000,
            port: 8080,
        }
    }
    async fn call(
        method: Method,
        uri: &str,
        body: Body,
        content_length: Option<usize>,
    ) -> Response {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(length) = content_length {
            request = request.header(header::CONTENT_LENGTH, length);
        }
        let mut request = request.body(body).unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 1234))));
        app(settings()).oneshot(request).await.unwrap()
    }
    async fn json_body(response: Response) -> Value {
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
    }

    #[tokio::test]
    async fn public_info_and_health_contract() {
        let info = call(Method::GET, "/api/v1/info", Body::empty(), None).await;
        assert_eq!(info.status(), StatusCode::OK);
        assert_eq!(
            json_body(info).await,
            json!({"name":"personal-agent-push-gateway","version":"2026.8.29","platforms":["android"],"max_body_bytes":4096,"daily_quota":500})
        );
        assert_eq!(
            json_body(call(Method::GET, "/healthz", Body::empty(), None).await).await,
            json!({"ok":true})
        );
    }

    #[tokio::test]
    async fn validation_precedes_configuration() {
        let invalid = call(Method::POST, "/api/v1/push", Body::from("{}"), Some(2)).await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(invalid).await, json!({"error":"invalid"}));
        let raw = br#"{"v":1,"push_token":"device-token","platform":"android","priority":"high","data":{"type":"chat_reply","payload_id":"run:abc"}}"#;
        let valid = call(
            Method::POST,
            "/api/v1/push",
            Body::from(raw.as_slice()),
            Some(raw.len()),
        )
        .await;
        assert_eq!(valid.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json_body(valid).await, json!({"error":"not_configured"}));
    }

    #[tokio::test]
    async fn content_length_cap_is_checked_before_body() {
        let response = call(Method::POST, "/api/v1/push", Body::empty(), Some(4097)).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json_body(response).await, json!({"error":"too_large"}));
    }

    #[tokio::test]
    async fn metrics_have_all_zero_series() {
        let response = call(Method::GET, "/metrics", Body::empty(), None).await;
        let text = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        assert!(text.contains("push_gateway_build_info{version=\"2026.8.29\"} 1"));
        assert!(
            text.contains(
                "push_gateway_sends_total{outcome=\"unregistered\",priority=\"normal\"} 0"
            )
        );
        assert!(text.contains("push_gateway_rejected_total{reason=\"store_unavailable\"} 0"));
    }
}
