use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Embedded Admin Control Center HTML
const ADMIN_HTML: &str = include_str!("admin.html");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WsEvent {
    pub event_type: String,
    pub message: String,
    pub payload: serde_json::Value,
}

#[derive(Clone)]
pub struct AdminState {
    pub db: SqlitePool,
    pub tx: broadcast::Sender<WsEvent>,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct AdminUser {
    pub id: i64,
    pub username: String,
    pub display_name: String,
    pub role: String,
    pub all_download_access: i64,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateAdminUserReq {
    pub username: String,
    pub display_name: String,
    pub role: String,
    pub all_download_access: Option<bool>,
    pub password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct AdminCommission {
    pub id: i64,
    pub title: String,
    pub client_id: i64,
    pub artist_id: i64,
    pub status: String,
    pub price: f64,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct AdminProduct {
    pub id: i64,
    pub artist_id: i64,
    pub title: String,
    pub min_price: f64,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct AdminEscrow {
    pub id: i64,
    pub amount: f64,
    pub status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "atelierv_admin=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Initializing AtelierV Admin Host (Port 7861)...");

    // Connect to the shared SQLite database
    let db_path = "sqlite:vgen_local.db?mode=rwc";
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(db_path)
        .await?;

    let (tx, _rx) = broadcast::channel::<WsEvent>(100);
    let state = AdminState { db: pool, tx };

    let app = Router::new()
        .route("/", get(serve_admin_ui))
        .route("/ws", get(admin_ws_handler))
        .route("/admin/api/users", get(list_users).post(create_user))
        .route("/admin/api/users/:id", delete(delete_user))
        .route("/admin/api/users/:id/toggle-access", post(toggle_user_access))
        .route("/admin/api/commissions", get(list_commissions))
        .route("/admin/api/commissions/:id/cancel", post(cancel_commission))
        .route("/admin/api/products", get(list_products))
        .route("/admin/api/escrow", get(list_escrow))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Dedicated Port 7861 for Admin
    let addr = SocketAddr::from(([127, 0, 0, 1], 7861));
    tracing::info!("🛡️ AtelierV Admin Dashboard listening on http://{}", addr);

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
        let url = "http://localhost:7861";
        tracing::info!("Opening admin control center in browser at {}...", url);
        let _ = open::that(url);
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn serve_admin_ui() -> impl IntoResponse {
    Html(ADMIN_HTML)
}

async fn list_users(State(state): State<AdminState>) -> Json<Vec<AdminUser>> {
    let users = sqlx::query_as::<_, AdminUser>(
        "SELECT id, username, display_name, role, all_download_access, created_at FROM users ORDER BY id ASC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(users)
}

async fn create_user(
    State(state): State<AdminState>,
    Json(p): Json<CreateAdminUserReq>,
) -> Response {
    let pass = p.password.unwrap_or_else(|| "password123".into());
    let all_access = if p.all_download_access.unwrap_or(false) { 1 } else { 0 };

    let res = sqlx::query(
        "INSERT INTO users (username, display_name, role, password_hash, all_download_access) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(p.username.trim().to_lowercase())
    .bind(&p.display_name)
    .bind(&p.role)
    .bind(&pass)
    .bind(all_access)
    .execute(&state.db)
    .await;

    match res {
        Ok(_) => {
            let _ = state.tx.send(WsEvent {
                event_type: "PERSONA_CREATED".into(),
                message: format!("Admin created account: {} (all_access: {})", p.username, all_access == 1),
                payload: serde_json::json!({ "username": p.username }),
            });
            Json(serde_json::json!({ "status": "ok" })).into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn delete_user(State(state): State<AdminState>, Path(id): Path<i64>) -> Response {
    let _ = sqlx::query("DELETE FROM users WHERE id = ?").bind(id).execute(&state.db).await;
    Json(serde_json::json!({ "status": "deleted" })).into_response()
}

async fn toggle_user_access(State(state): State<AdminState>, Path(id): Path<i64>) -> Response {
    let current: Option<(i64,)> = sqlx::query_as("SELECT all_download_access FROM users WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);

    match current {
        Some((val,)) => {
            let next = if val == 1 { 0i64 } else { 1i64 };
            let _ = sqlx::query("UPDATE users SET all_download_access = ? WHERE id = ?")
                .bind(next)
                .bind(id)
                .execute(&state.db)
                .await;
            let _ = state.tx.send(WsEvent {
                event_type: "ACCESS_TOGGLED".into(),
                message: format!("Admin {} All-Download Pass for user #{}", if next == 1 { "GRANTED" } else { "REVOKED" }, id),
                payload: serde_json::json!({ "user_id": id, "all_download_access": next == 1 }),
            });
            Json(serde_json::json!({ "status": "toggled", "all_download_access": next == 1 })).into_response()
        }
        None => (axum::http::StatusCode::NOT_FOUND, "User not found").into_response(),
    }
}

async fn list_commissions(State(state): State<AdminState>) -> Json<Vec<AdminCommission>> {
    let comms = sqlx::query_as::<_, AdminCommission>(
        "SELECT id, title, client_id, artist_id, status, price, created_at FROM commissions ORDER BY id DESC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(comms)
}

async fn cancel_commission(State(state): State<AdminState>, Path(id): Path<i64>) -> Response {
    let _ = sqlx::query("UPDATE commissions SET status = 'CANCELLED' WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await;

    let _ = sqlx::query("UPDATE escrow_ledger SET status = 'REFUNDED' WHERE commission_id = ?")
        .bind(id)
        .execute(&state.db)
        .await;

    Json(serde_json::json!({ "status": "cancelled" })).into_response()
}

async fn list_products(State(state): State<AdminState>) -> Json<Vec<AdminProduct>> {
    let prods = sqlx::query_as::<_, AdminProduct>(
        "SELECT id, artist_id, title, min_price, created_at FROM digital_products ORDER BY id DESC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(prods)
}

async fn list_escrow(State(state): State<AdminState>) -> Json<Vec<AdminEscrow>> {
    let escrow = sqlx::query_as::<_, AdminEscrow>(
        "SELECT id, amount, status FROM escrow_ledger"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(escrow)
}

async fn admin_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_admin_socket(socket, state))
}

async fn handle_admin_socket(socket: WebSocket, state: AdminState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if let Ok(serialized) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(serialized)).await.is_err() {
                    break;
                }
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(_))) = receiver.next().await {}
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}
