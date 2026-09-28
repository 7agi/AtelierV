use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Embedded single-page dashboard
const DASHBOARD_HTML: &str = include_str!("index.html");

// Broadcast event payload for real-time WebSocket updates
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WsEvent {
    pub event_type: String,
    pub message: String,
    pub payload: serde_json::Value,
}

// Global App State
#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    pub tx: broadcast::Sender<WsEvent>,
}

// ---------------------------------------------------------
// DATABASE MODELS & DTOs
// ---------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub display_name: String,
    pub role: String, // Admin, Artist, Client
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserReq {
    pub username: String,
    pub display_name: String,
    pub role: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Commission {
    pub id: i64,
    pub title: String,
    pub client_id: i64,
    pub artist_id: i64,
    pub status: String, // REQUESTED, IN_PROGRESS, IN_REVIEW, COMPLETED, CANCELLED
    pub price: f64,
    pub brief: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateCommissionReq {
    pub title: String,
    pub client_id: i64,
    pub artist_id: i64,
    pub price: f64,
    pub brief: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStatusReq {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct CompleteCommReq {
    pub character_name: String,
    pub license_type: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct EscrowLedger {
    pub id: i64,
    pub commission_id: Option<i64>,
    pub client_id: i64,
    pub artist_id: i64,
    pub amount: f64,
    pub status: String, // HELD_IN_ESCROW, RELEASED_TO_ARTIST, REFUNDED
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct DepositReq {
    pub client_id: i64,
    pub amount: f64,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct OwnershipLog {
    pub id: i64,
    pub commission_id: i64,
    pub character_name: String,
    pub owner_user_id: i64,
    pub creator_user_id: i64,
    pub license_type: String,
    pub certificate_hash: String,
    pub registered_at: String,
}

// ---------------------------------------------------------
// MAIN INITIALIZATION & STARTUP
// ---------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "atelierv=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Initializing AtelierV Local Engine...");

    // 2. Setup SQLite database file & connection pool
    let db_path = "sqlite:vgen_local.db?mode=rwc";
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(db_path)
        .await?;

    // 3. Run auto-migrations
    run_migrations(&pool).await?;

    // 4. Seed initial personas if empty
    seed_database(&pool).await?;

    // 5. Setup broadcast channel for WebSockets
    let (tx, _rx) = broadcast::channel::<WsEvent>(100);

    let state = AppState { db: pool, tx };

    // 6. Router definition
    let app = Router::new()
        // Embedded dashboard
        .route("/", get(serve_dashboard))
        // WebSocket live sync endpoint
        .route("/ws", get(ws_handler))
        // REST API
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/:id", delete(delete_user))
        .route("/api/commissions", get(list_commissions).post(create_commission))
        .route("/api/commissions/:id/status", put(update_commission_status))
        .route("/api/commissions/:id/complete", post(complete_commission))
        .route("/api/escrow", get(list_escrow))
        .route("/api/escrow/deposit", post(test_deposit))
        .route("/api/escrow/:id/release", post(release_escrow))
        .route("/api/ownership", get(list_ownership))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // 7. Bind server to 127.0.0.1:7860
    let addr = SocketAddr::from(([127, 0, 0, 1], 7860));
    tracing::info!("🚀 AtelierV server listening on http://{}", addr);

    // 8. Auto-open default web browser
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
        let url = "http://localhost:7860";
        tracing::info!("Opening browser at {}...", url);
        if let Err(e) = open::that(url) {
            tracing::warn!("Failed to auto-open browser: {}. Please navigate manually.", e);
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ---------------------------------------------------------
// DATABASE SCHEMA & SEEDING
// ---------------------------------------------------------

async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS commissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            client_id INTEGER NOT NULL,
            artist_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            price REAL NOT NULL,
            brief TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(client_id) REFERENCES users(id),
            FOREIGN KEY(artist_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS escrow_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commission_id INTEGER,
            client_id INTEGER NOT NULL,
            artist_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(commission_id) REFERENCES commissions(id),
            FOREIGN KEY(client_id) REFERENCES users(id),
            FOREIGN KEY(artist_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS ownership_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commission_id INTEGER NOT NULL,
            character_name TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL,
            creator_user_id INTEGER NOT NULL,
            license_type TEXT NOT NULL,
            certificate_hash TEXT NOT NULL,
            registered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(commission_id) REFERENCES commissions(id),
            FOREIGN KEY(owner_user_id) REFERENCES users(id),
            FOREIGN KEY(creator_user_id) REFERENCES users(id)
        );
        "#,
    )
    .execute(pool)
    .await?;

    tracing::info!("Database schema validated.");
    Ok(())
}

async fn seed_database(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(pool)
        .await?;

    if count.0 == 0 {
        tracing::info!("Seeding initial personas (Admin, Artist, Client)...");
        sqlx::query(
            r#"
            INSERT INTO users (username, display_name, role) VALUES 
            ('admin_nexus', 'Atelier Director (Admin)', 'Admin'),
            ('lyra_v', 'Lyra - 2D Live Rigging (Artist)', 'Artist'),
            ('kuro_stream', 'Kuro Ch. (Client)', 'Client');
            "#,
        )
        .execute(pool)
        .await?;

        // Seed a sample initial commission
        sqlx::query(
            r#"
            INSERT INTO commissions (title, client_id, artist_id, status, price, brief) VALUES
            ('Debut Vtuber Rig & Expressions', 3, 2, 'IN_PROGRESS', 550.00, 'Full body rig with 5 toggle expressions and physics for long flowing hair.');
            "#,
        )
        .execute(pool)
        .await?;

        // Seed sample escrow
        sqlx::query(
            r#"
            INSERT INTO escrow_ledger (commission_id, client_id, artist_id, amount, status) VALUES
            (1, 3, 2, 550.00, 'HELD_IN_ESCROW');
            "#,
        )
        .execute(pool)
        .await?;

        tracing::info!("Default database seeding complete.");
    }

    Ok(())
}

// ---------------------------------------------------------
// REST API & HANDLERS
// ---------------------------------------------------------

async fn serve_dashboard() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

async fn list_users(State(state): State<AppState>) -> Json<Vec<User>> {
    let users = sqlx::query_as::<_, User>("SELECT id, username, display_name, role, created_at FROM users ORDER BY id ASC")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    Json(users)
}

async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<CreateUserReq>,
) -> Response {
    let res = sqlx::query(
        "INSERT INTO users (username, display_name, role) VALUES (?, ?, ?)"
    )
    .bind(&payload.username)
    .bind(&payload.display_name)
    .bind(&payload.role)
    .execute(&state.db)
    .await;

    match res {
        Ok(r) => {
            let id = r.last_insert_rowid();
            let _ = state.tx.send(WsEvent {
                event_type: "PERSONA_CREATED".into(),
                message: format!("New persona registered: {} (@{})", payload.display_name, payload.role),
                payload: serde_json::json!({ "id": id, "username": payload.username }),
            });
            Json(serde_json::json!({ "id": id, "status": "ok" })).into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn delete_user(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let _ = sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await;

    let _ = state.tx.send(WsEvent {
        event_type: "PERSONA_DELETED".into(),
        message: format!("Persona #{} removed", id),
        payload: serde_json::json!({ "id": id }),
    });

    Json(serde_json::json!({ "status": "deleted" })).into_response()
}

async fn list_commissions(State(state): State<AppState>) -> Json<Vec<Commission>> {
    let comms = sqlx::query_as::<_, Commission>(
        "SELECT id, title, client_id, artist_id, status, price, brief, created_at, updated_at FROM commissions ORDER BY id DESC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(comms)
}

async fn create_commission(
    State(state): State<AppState>,
    Json(payload): Json<CreateCommissionReq>,
) -> Response {
    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let res = sqlx::query(
        "INSERT INTO commissions (title, client_id, artist_id, status, price, brief) VALUES (?, ?, ?, 'REQUESTED', ?, ?)"
    )
    .bind(&payload.title)
    .bind(payload.client_id)
    .bind(payload.artist_id)
    .bind(payload.price)
    .bind(&payload.brief)
    .execute(&mut *tx)
    .await;

    let comm_id = match res {
        Ok(r) => r.last_insert_rowid(),
        Err(e) => return (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Auto-create initial Escrow lock
    let _ = sqlx::query(
        "INSERT INTO escrow_ledger (commission_id, client_id, artist_id, amount, status) VALUES (?, ?, ?, ?, 'HELD_IN_ESCROW')"
    )
    .bind(comm_id)
    .bind(payload.client_id)
    .bind(payload.artist_id)
    .bind(payload.price)
    .execute(&mut *tx)
    .await;

    if let Err(e) = tx.commit().await {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let _ = state.tx.send(WsEvent {
        event_type: "COMMISSION_CREATED".into(),
        message: format!("New brief '#{} - {}' placed into escrow (${:.2})", comm_id, payload.title, payload.price),
        payload: serde_json::json!({ "id": comm_id, "title": payload.title }),
    });

    Json(serde_json::json!({ "id": comm_id, "status": "created" })).into_response()
}

async fn update_commission_status(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<UpdateStatusReq>,
) -> Response {
    let _ = sqlx::query("UPDATE commissions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(&payload.status)
        .bind(id)
        .execute(&state.db)
        .await;

    let _ = state.tx.send(WsEvent {
        event_type: "STATUS_SHIFT".into(),
        message: format!("Commission #{} moved to {}", id, payload.status),
        payload: serde_json::json!({ "id": id, "status": payload.status }),
    });

    Json(serde_json::json!({ "status": "updated" })).into_response()
}

async fn complete_commission(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<CompleteCommReq>,
) -> Response {
    let comm = sqlx::query_as::<_, Commission>("SELECT * FROM commissions WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);

    let comm = match comm {
        Some(c) => c,
        None => return (axum::http::StatusCode::NOT_FOUND, "Commission not found").into_response(),
    };

    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    // 1. Mark commission completed
    let _ = sqlx::query("UPDATE commissions SET status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await;

    // 2. Release escrow to artist
    let _ = sqlx::query("UPDATE escrow_ledger SET status = 'RELEASED_TO_ARTIST' WHERE commission_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await;

    // 3. Generate SHA256-style legal certificate hash
    let cert_hash = format!(
        "CERT-{:x}-{}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        comm.client_id,
        id
    );

    // 4. Log permanent copyright ownership registry
    let _ = sqlx::query(
        "INSERT INTO ownership_logs (commission_id, character_name, owner_user_id, creator_user_id, license_type, certificate_hash) VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(id)
    .bind(&payload.character_name)
    .bind(comm.client_id)
    .bind(comm.artist_id)
    .bind(&payload.license_type)
    .bind(&cert_hash)
    .execute(&mut *tx)
    .await;

    if let Err(e) = tx.commit().await {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let _ = state.tx.send(WsEvent {
        event_type: "ESCROW_PAYOUT".into(),
        message: format!("Escrow funds for Commission #{} released to artist. Deliverables certified!", id),
        payload: serde_json::json!({ "id": id, "hash": cert_hash }),
    });

    Json(serde_json::json!({ "status": "completed", "certificate": cert_hash })).into_response()
}

async fn list_escrow(State(state): State<AppState>) -> Json<Vec<EscrowLedger>> {
    let ledger = sqlx::query_as::<_, EscrowLedger>(
        "SELECT id, commission_id, client_id, artist_id, amount, status, created_at FROM escrow_ledger ORDER BY id DESC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(ledger)
}

async fn test_deposit(
    State(state): State<AppState>,
    Json(payload): Json<DepositReq>,
) -> Response {
    let _ = sqlx::query(
        "INSERT INTO escrow_ledger (commission_id, client_id, artist_id, amount, status) VALUES (NULL, ?, 1, ?, 'HELD_IN_ESCROW')"
    )
    .bind(payload.client_id)
    .bind(payload.amount)
    .execute(&state.db)
    .await;

    let _ = state.tx.send(WsEvent {
        event_type: "ESCROW_DEPOSIT".into(),
        message: format!("User #{} deposited ${:.2} into escrow balance pool", payload.client_id, payload.amount),
        payload: serde_json::json!({ "amount": payload.amount }),
    });

    Json(serde_json::json!({ "status": "deposited" })).into_response()
}

async fn release_escrow(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let _ = sqlx::query("UPDATE escrow_ledger SET status = 'RELEASED_TO_ARTIST' WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await;

    let _ = state.tx.send(WsEvent {
        event_type: "ESCROW_RELEASE".into(),
        message: format!("Escrow entry #{} manually settled and disbursed", id),
        payload: serde_json::json!({ "id": id }),
    });

    Json(serde_json::json!({ "status": "released" })).into_response()
}

async fn list_ownership(State(state): State<AppState>) -> Json<Vec<OwnershipLog>> {
    let logs = sqlx::query_as::<_, OwnershipLog>(
        "SELECT id, commission_id, character_name, owner_user_id, creator_user_id, license_type, certificate_hash, registered_at FROM ownership_logs ORDER BY id DESC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    Json(logs)
}

// ---------------------------------------------------------
// WEBSOCKET REAL-TIME STREAMING
// ---------------------------------------------------------

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    // Task forwarding broadcast messages to the connected browser
    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if let Ok(serialized) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(serialized)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Task reading any client incoming WS pings / chats
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            tracing::debug!("Received ws text: {}", text);
        }
    });

    // If either finishes, abort the other
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}
