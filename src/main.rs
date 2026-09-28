use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
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
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SafeUser {
    pub id: i64,
    pub username: String,
    pub display_name: String,
    pub role: String,
    pub created_at: String,
}

impl From<User> for SafeUser {
    fn from(u: User) -> Self {
        SafeUser {
            id: u.id,
            username: u.username,
            display_name: u.display_name,
            role: u.role,
            created_at: u.created_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SignupReq {
    pub username: String,
    pub display_name: String,
    pub role: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginReq {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct UserQuery {
    pub user_id: Option<i64>,
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
    pub note: Option<String>,
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

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct CommissionMilestone {
    pub id: i64,
    pub commission_id: i64,
    pub stage_name: String,
    pub note: String,
    pub created_at: String,
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
        // Auth API
        .route("/api/auth/signup", post(signup_user))
        .route("/api/auth/login", post(login_user))
        .route("/api/auth/me", get(get_current_user))
        // User Persona & Public Artist directory
        .route("/api/users", get(list_users))
        .route("/api/users/artists", get(list_artists))
        .route("/api/users/:id", delete(delete_user))
        // Role-filtered Commissions & Workflows
        .route("/api/commissions", get(list_commissions).post(create_commission))
        .route("/api/commissions/:id/status", put(update_commission_status))
        .route("/api/commissions/:id/complete", post(complete_commission))
        .route("/api/commissions/:id/milestones", get(get_commission_milestones))
        // Escrow & Ownership
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
            password_hash TEXT NOT NULL DEFAULT 'password',
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

        CREATE TABLE IF NOT EXISTS commission_milestones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commission_id INTEGER NOT NULL,
            stage_name TEXT NOT NULL,
            note TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(commission_id) REFERENCES commissions(id)
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

    // Attempt to add password_hash column in case older DB exists without it
    let _ = sqlx::query("ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT 'password'")
        .execute(pool)
        .await;

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
            INSERT INTO users (username, display_name, role, password_hash) VALUES 
            ('admin', 'Atelier Director (Admin)', 'Admin', 'admin123'),
            ('lyra', 'Lyra - 2D Live Rigging (Artist)', 'Artist', 'artist123'),
            ('kuro', 'Kuro Ch. (Client)', 'Client', 'client123');
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

        // Seed sample milestone progress
        sqlx::query(
            r#"
            INSERT INTO commission_milestones (commission_id, stage_name, note) VALUES
            (1, 'REQUESTED', 'Brief initiated and safe escrow locked by client.'),
            (1, 'IN_PROGRESS', 'Artist accepted request and began base mesh splitting.');
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

// Auth: Signup
async fn signup_user(
    State(state): State<AppState>,
    Json(payload): Json<SignupReq>,
) -> Response {
    let clean_user = payload.username.trim().to_lowercase();
    if clean_user.is_empty() || payload.password.trim().is_empty() {
        return (axum::http::StatusCode::BAD_REQUEST, "Username and password are required").into_response();
    }

    let role = match payload.role.as_str() {
        "Artist" => "Artist",
        "Admin" => "Admin",
        _ => "Client",
    };

    let res = sqlx::query(
        "INSERT INTO users (username, display_name, role, password_hash) VALUES (?, ?, ?, ?)"
    )
    .bind(&clean_user)
    .bind(&payload.display_name)
    .bind(role)
    .bind(&payload.password)
    .execute(&state.db)
    .await;

    match res {
        Ok(r) => {
            let id = r.last_insert_rowid();
            let _ = state.tx.send(WsEvent {
                event_type: "PERSONA_CREATED".into(),
                message: format!("New {} registered: {} (@{})", role, payload.display_name, clean_user),
                payload: serde_json::json!({ "id": id, "username": clean_user, "role": role }),
            });
            Json(serde_json::json!({
                "status": "ok",
                "user": {
                    "id": id,
                    "username": clean_user,
                    "display_name": payload.display_name,
                    "role": role
                }
            })).into_response()
        }
        Err(e) => (axum::http::StatusCode::CONFLICT, format!("Username already taken or database error: {}", e)).into_response(),
    }
}

// Auth: Login
async fn login_user(
    State(state): State<AppState>,
    Json(payload): Json<LoginReq>,
) -> Response {
    let clean_user = payload.username.trim().to_lowercase();
    let user_opt = sqlx::query_as::<_, User>(
        "SELECT id, username, display_name, role, password_hash, created_at FROM users WHERE LOWER(username) = ?"
    )
    .bind(&clean_user)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    match user_opt {
        Some(user) => {
            if user.password_hash == payload.password {
                let safe = SafeUser::from(user);
                Json(serde_json::json!({
                    "status": "ok",
                    "user": safe
                })).into_response()
            } else {
                (axum::http::StatusCode::UNAUTHORIZED, "Invalid username or password").into_response()
            }
        }
        None => (axum::http::StatusCode::UNAUTHORIZED, "Invalid username or password").into_response(),
    }
}

// Auth: /api/auth/me?user_id=1
async fn get_current_user(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(id) => id,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id query parameter").into_response(),
    };

    let user_opt = sqlx::query_as::<_, User>(
        "SELECT id, username, display_name, role, password_hash, created_at FROM users WHERE id = ?"
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    match user_opt {
        Some(u) => Json(SafeUser::from(u)).into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "User not found").into_response(),
    }
}

async fn list_users(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    // Only Admin can view the full persona directory
    if let Some(uid) = q.user_id {
        let role: Option<(String,)> = sqlx::query_as("SELECT role FROM users WHERE id = ?")
            .bind(uid)
            .fetch_optional(&state.db)
            .await
            .unwrap_or(None);
        
        if let Some((r,)) = role {
            if r == "Admin" {
                let users = sqlx::query_as::<_, User>(
                    "SELECT id, username, display_name, role, password_hash, created_at FROM users ORDER BY id ASC"
                )
                .fetch_all(&state.db)
                .await
                .unwrap_or_default();
                let safe_users: Vec<SafeUser> = users.into_iter().map(SafeUser::from).collect();
                return Json(safe_users).into_response();
            }
        }
    }

    (axum::http::StatusCode::FORBIDDEN, "Access denied: Persona directory restricted to Admin").into_response()
}

// Public artist listing so clients can choose an artist when commissioning
async fn list_artists(State(state): State<AppState>) -> Json<Vec<SafeUser>> {
    let artists = sqlx::query_as::<_, User>(
        "SELECT id, username, display_name, role, password_hash, created_at FROM users WHERE role = 'Artist' ORDER BY display_name ASC"
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let safe: Vec<SafeUser> = artists.into_iter().map(SafeUser::from).collect();
    Json(safe)
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

// Role-Scoped Commissions Listing
async fn list_commissions(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(uid) => uid,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id parameter").into_response(),
    };

    let user_opt = sqlx::query_as::<_, User>(
        "SELECT id, username, display_name, role, password_hash, created_at FROM users WHERE id = ?"
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let role = match user_opt {
        Some(u) => u.role,
        None => return (axum::http::StatusCode::UNAUTHORIZED, "Invalid user session").into_response(),
    };

    let comms = if role == "Admin" {
        // Admin sees all commissions across the system
        sqlx::query_as::<_, Commission>(
            "SELECT id, title, client_id, artist_id, status, price, brief, created_at, updated_at FROM commissions ORDER BY id DESC"
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    } else if role == "Artist" {
        // Artists only see what THEY receive / put out
        sqlx::query_as::<_, Commission>(
            "SELECT id, title, client_id, artist_id, status, price, brief, created_at, updated_at FROM commissions WHERE artist_id = ? ORDER BY id DESC"
        )
        .bind(user_id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    } else {
        // Clients only see what THEY request
        sqlx::query_as::<_, Commission>(
            "SELECT id, title, client_id, artist_id, status, price, brief, created_at, updated_at FROM commissions WHERE client_id = ? ORDER BY id DESC"
        )
        .bind(user_id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    };

    Json(comms).into_response()
}

// Fetch in-depth milestone history for a specific commission
async fn get_commission_milestones(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Json<Vec<CommissionMilestone>> {
    let milestones = sqlx::query_as::<_, CommissionMilestone>(
        "SELECT id, commission_id, stage_name, note, created_at FROM commission_milestones WHERE commission_id = ? ORDER BY id ASC"
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Json(milestones)
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

    // Record first milestone
    let _ = sqlx::query(
        "INSERT INTO commission_milestones (commission_id, stage_name, note) VALUES (?, 'REQUESTED', 'Brief launched by client and funds held in safe escrow.')"
    )
    .bind(comm_id)
    .execute(&mut *tx)
    .await;

    if let Err(e) = tx.commit().await {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let _ = state.tx.send(WsEvent {
        event_type: "COMMISSION_CREATED".into(),
        message: format!("New brief '#{} - {}' placed into escrow (${:.2})", comm_id, payload.title, payload.price),
        payload: serde_json::json!({
            "id": comm_id,
            "title": payload.title,
            "client_id": payload.client_id,
            "artist_id": payload.artist_id
        }),
    });

    Json(serde_json::json!({ "id": comm_id, "status": "created" })).into_response()
}

async fn update_commission_status(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<UpdateStatusReq>,
) -> Response {
    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let _ = sqlx::query("UPDATE commissions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(&payload.status)
        .bind(id)
        .execute(&mut *tx)
        .await;

    let note_text = payload.note.unwrap_or_else(|| match payload.status.as_str() {
        "IN_PROGRESS" => "Artist accepted the brief and began creative production.",
        "IN_REVIEW" => "Artist submitted milestone drafts and previews for client feedback.",
        "COMPLETED" => "Client approved all deliverables and closed the request.",
        "CANCELLED" => "Commission was cancelled.",
        _ => "Status updated.",
    }.to_string());

    let _ = sqlx::query(
        "INSERT INTO commission_milestones (commission_id, stage_name, note) VALUES (?, ?, ?)"
    )
    .bind(id)
    .bind(&payload.status)
    .bind(&note_text)
    .execute(&mut *tx)
    .await;

    if let Err(e) = tx.commit().await {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let _ = state.tx.send(WsEvent {
        event_type: "STATUS_SHIFT".into(),
        message: format!("Commission #{} moved to {}", id, payload.status),
        payload: serde_json::json!({ "id": id, "status": payload.status, "note": note_text }),
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

    // 5. Milestone log
    let milestone_note = format!("Approved by client! IP Certificate '{}' registered and funds settled.", cert_hash);
    let _ = sqlx::query(
        "INSERT INTO commission_milestones (commission_id, stage_name, note) VALUES (?, 'COMPLETED', ?)"
    )
    .bind(id)
    .bind(&milestone_note)
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

// Escrow listing scoped by user
async fn list_escrow(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(uid) => uid,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id parameter").into_response(),
    };

    let user_opt = sqlx::query_as::<_, User>(
        "SELECT id, username, display_name, role, password_hash, created_at FROM users WHERE id = ?"
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let role = match user_opt {
        Some(u) => u.role,
        None => return (axum::http::StatusCode::UNAUTHORIZED, "Invalid user session").into_response(),
    };

    let ledger = if role == "Admin" {
        sqlx::query_as::<_, EscrowLedger>(
            "SELECT id, commission_id, client_id, artist_id, amount, status, created_at FROM escrow_ledger ORDER BY id DESC"
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    } else {
        sqlx::query_as::<_, EscrowLedger>(
            "SELECT id, commission_id, client_id, artist_id, amount, status, created_at FROM escrow_ledger WHERE client_id = ? OR artist_id = ? ORDER BY id DESC"
        )
        .bind(user_id)
        .bind(user_id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    };

    Json(ledger).into_response()
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

// Ownership listing scoped by user
async fn list_ownership(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(uid) => uid,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id parameter").into_response(),
    };

    let user_opt = sqlx::query_as::<_, User>(
        "SELECT id, username, display_name, role, password_hash, created_at FROM users WHERE id = ?"
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let role = match user_opt {
        Some(u) => u.role,
        None => return (axum::http::StatusCode::UNAUTHORIZED, "Invalid user session").into_response(),
    };

    let logs = if role == "Admin" {
        sqlx::query_as::<_, OwnershipLog>(
            "SELECT id, commission_id, character_name, owner_user_id, creator_user_id, license_type, certificate_hash, registered_at FROM ownership_logs ORDER BY id DESC"
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    } else {
        sqlx::query_as::<_, OwnershipLog>(
            "SELECT id, commission_id, character_name, owner_user_id, creator_user_id, license_type, certificate_hash, registered_at FROM ownership_logs WHERE owner_user_id = ? OR creator_user_id = ? ORDER BY id DESC"
        )
        .bind(user_id)
        .bind(user_id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    };

    Json(logs).into_response()
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
