use push_gateway::config::Settings;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("push_gateway=info".parse().unwrap()),
        )
        .init();
    let settings = Settings::from_env();
    let port = settings.port;
    let app = push_gateway::app(settings);
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind listener");
    tracing::info!(
        version = push_gateway::VERSION,
        port,
        "push gateway starting"
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown())
    .await
    .expect("server failed");
}
async fn shutdown() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.expect("ctrl-c handler") };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {_=ctrl_c=>{},_=terminate=>{}}
}
