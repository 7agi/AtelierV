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

// Embedded client/artist portal HTML
const DASHBOARD_HTML: &str = include_str!("index.html");

// Broadcast event payload for real-time WebSocket updates
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WsEvent {
    pub event_type: String,
    pub message: String,
    pub payload: serde_json::Value,
}

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
    pub role: String, // Artist, Client, Admin
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

// Digital Products Marketplace Models
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct DigitalProduct {
    pub id: i64,
    pub artist_id: i64,
    pub title: String,
    pub description: String,
    pub media_url: String, // Supports jpg, png, webp, gif, or data url
    pub file_download_url: String,
    pub min_price: f64,    // Fixed baseline price
    pub vendor_info: String, // Helpful delivery, specs, usage info
    pub score_weight: f64, // Internal weight for discovery algorithm
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DigitalProductWithStats {
    pub id: i64,
    pub artist_id: i64,
    pub artist_name: String,
    pub artist_username: String,
    pub title: String,
    pub description: String,
    pub media_url: String,
    pub file_download_url: String,
    pub min_price: f64,
    pub vendor_info: String,
    pub average_rating: f64,
    pub review_count: i64,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateProductReq {
    pub artist_id: i64,
    pub title: String,
    pub description: String,
    pub media_url: String,
    pub file_download_url: Option<String>,
    pub min_price: f64,
    pub vendor_info: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct ProductReview {
    pub id: i64,
    pub product_id: i64,
    pub user_id: i64,
    pub user_name: String,
    pub rating: i64, // 1 to 5
    pub comment: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateReviewReq {
    pub product_id: i64,
    pub user_id: i64,
    pub rating: i64,
    pub comment: String,
}

#[derive(Debug, Deserialize)]
pub struct PurchaseProductReq {
    pub product_id: i64,
    pub client_id: i64,
    pub offered_price: f64, // Can be >= min_price (pay what you want)
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct ProductPurchase {
    pub id: i64,
    pub product_id: i64,
    pub client_id: i64,
    pub amount_paid: f64,
    pub product_title: String,
    pub file_download_url: String,
    pub purchased_at: String,
}

// Commission & Workflow Models
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Commission {
    pub id: i64,
    pub title: String,
    pub client_id: i64,
    pub artist_id: i64,
    pub status: String,
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
pub struct CommissionMilestone {
    pub id: i64,
    pub commission_id: i64,
    pub stage_name: String,
    pub note: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct EscrowLedger {
    pub id: i64,
    pub commission_id: Option<i64>,
    pub client_id: i64,
    pub artist_id: i64,
    pub amount: f64,
    pub status: String,
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
// MAIN INITIALIZATION (Port 7860: Client & Artist Portal)
// ---------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "atelierv=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Initializing AtelierV User & Artist Engine...");

    let db_path = "sqlite:vgen_local.db?mode=rwc";
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(db_path)
        .await?;

    run_migrations(&pool).await?;
    seed_database(&pool).await?;

    let (tx, _rx) = broadcast::channel::<WsEvent>(100);
    let state = AppState { db: pool, tx };

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/ws", get(ws_handler))
        // Auth API
        .route("/api/auth/signup", post(signup_user))
        .route("/api/auth/login", post(login_user))
        .route("/api/auth/me", get(get_current_user))
        // Artists directory
        .route("/api/users/artists", get(list_artists))
        // Digital Products Marketplace
        .route("/api/products", get(list_products).post(create_product))
        .route("/api/products/:id", get(get_product_detail))
        .route("/api/products/:id/reviews", get(list_reviews).post(add_review))
        .route("/api/products/:id/purchase", post(purchase_product))
        .route("/api/purchases", get(list_user_purchases))
        // Commissions & Milestones
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

    let addr = SocketAddr::from(([127, 0, 0, 1], 7860));
    tracing::info!("🚀 AtelierV User Portal listening on http://{}", addr);

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
        let url = "http://localhost:7860";
        tracing::info!("Opening browser at {}...", url);
        if let Err(e) = open::that(url) {
            tracing::warn!("Auto-open failed: {}", e);
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ---------------------------------------------------------
// DATABASE SCHEMA & AUTO-MIGRATIONS
// ---------------------------------------------------------

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::Error> {
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

        CREATE TABLE IF NOT EXISTS digital_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            media_url TEXT NOT NULL,
            file_download_url TEXT NOT NULL,
            min_price REAL NOT NULL,
            vendor_info TEXT NOT NULL,
            score_weight REAL NOT NULL DEFAULT 1.0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(artist_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS product_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            user_name TEXT NOT NULL,
            rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
            comment TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES digital_products(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS product_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            client_id INTEGER NOT NULL,
            amount_paid REAL NOT NULL,
            purchased_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES digital_products(id),
            FOREIGN KEY(client_id) REFERENCES users(id)
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

    let _ = sqlx::query("ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT 'password'")
        .execute(pool)
        .await;

    tracing::info!("Database schema validated.");
    Ok(())
}

pub async fn seed_database(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(pool)
        .await?;

    if count.0 == 0 {
        tracing::info!("Seeding initial personas and demo digital products...");
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

        // Seed initial digital products with images/gifs and vendor info
        sqlx::query(
            r#"
            INSERT INTO digital_products (artist_id, title, description, media_url, file_download_url, min_price, vendor_info, score_weight) VALUES
            (2, 'Cyberpunk Vtuber Emote Pack [Animated GIF]', 'Set of 6 high-res animated cyber cat emotes ready for Twitch/Discord.', 'https://images.unsplash.com/photo-1618005182384-a83a8bd57fbe?w=600&auto=format&fit=crop&q=80', 'https://example.com/downloads/cyber_emotes.zip', 25.00, 'Pack includes 112x112, 56x56, and 28x28 GIF & APNG files. Commercial stream license included. No re-selling.', 1.4),
            (2, 'Chibi Cat Ear 3D Model Preset', 'Pre-rigged VRChat / Live3D asset with dynamic bones & physics toggle.', 'https://images.unsplash.com/photo-1634017839464-5c339ebe3cb4?w=600&auto=format&fit=crop&q=80', 'https://example.com/downloads/cat_ears_preset.unitypackage', 15.00, 'Compatible with Unity 2022+ and VRChat SDK3. Blendshapes included for ear drooping/perking up.', 1.8),
            (2, 'Atmospheric Lo-Fi Stream Overlay Pack', 'Full neon night ambient overlay package with animated stinger transition.', 'https://images.unsplash.com/photo-1579783902614-a3fb3927b675?w=600&auto=format&fit=crop&q=80', 'https://example.com/downloads/lofi_overlay.zip', 35.00, 'Ready for OBS & Streamlabs. Features 1080p 60fps WebM transparent stinger, chatbox CSS, and starting screens.', 1.1);
            "#,
        )
        .execute(pool)
        .await?;

        // Seed initial reviews
        sqlx::query(
            r#"
            INSERT INTO product_reviews (product_id, user_id, user_name, rating, comment) VALUES
            (1, 3, 'Kuro Ch.', 5, 'The animated sparkles on the cat emote look insane on stream! Highly recommended.'),
            (2, 3, 'Kuro Ch.', 5, 'Rigged effortlessly onto my avatar in 5 minutes.');
            "#,
        )
        .execute(pool)
        .await?;

        // Seed initial sample commission
        sqlx::query(
            r#"
            INSERT INTO commissions (title, client_id, artist_id, status, price, brief) VALUES
            ('Debut Vtuber Rig & Expressions', 3, 2, 'IN_PROGRESS', 550.00, 'Full body rig with 5 toggle expressions and physics for long flowing hair.');
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO commission_milestones (commission_id, stage_name, note) VALUES
            (1, 'REQUESTED', 'Brief initiated and safe escrow locked by client.'),
            (1, 'IN_PROGRESS', 'Artist accepted request and began base mesh splitting.');
            "#,
        )
        .execute(pool)
        .await?;

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
        Err(e) => (axum::http::StatusCode::CONFLICT, format!("Username taken or DB error: {}", e)).into_response(),
    }
}

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

// ---------------------------------------------------------
// DIGITAL PRODUCTS & MARKETPLACE
// ---------------------------------------------------------

// Curated Discovery Algorithm: Not simple first-to-post.
// Weighted by: (Average Rating * Review Count) + internal discovery score + recency jitter
async fn list_products(State(state): State<AppState>) -> Json<Vec<DigitalProductWithStats>> {
    let rows: Vec<(i64, i64, String, String, String, String, String, String, f64, String, Option<f64>, i64)> = sqlx::query_as(
        r#"
        SELECT 
            p.id, p.artist_id, u.display_name as artist_name, u.username as artist_username,
            p.title, p.description, p.media_url, p.file_download_url, p.min_price, p.vendor_info,
            AVG(r.rating) as avg_rating,
            COUNT(r.id) as review_cnt
        FROM digital_products p
        JOIN users u ON p.artist_id = u.id
        LEFT JOIN product_reviews r ON p.id = r.product_id
        GROUP BY p.id
        ORDER BY 
            (COALESCE(AVG(r.rating), 4.0) * (COUNT(r.id) + 1) * p.score_weight) DESC,
            p.id DESC
        "#,
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let results = rows
        .into_iter()
        .map(|r| DigitalProductWithStats {
            id: r.0,
            artist_id: r.1,
            artist_name: r.2,
            artist_username: r.3,
            title: r.4,
            description: r.5,
            media_url: r.6,
            file_download_url: r.7,
            min_price: r.8,
            vendor_info: r.9,
            average_rating: r.10.unwrap_or(5.0),
            review_count: r.11,
            created_at: "".into(),
        })
        .collect();

    Json(results)
}

async fn get_product_detail(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Response {
    let row: Option<(i64, i64, String, String, String, String, String, String, f64, String, Option<f64>, i64)> = sqlx::query_as(
        r#"
        SELECT 
            p.id, p.artist_id, u.display_name, u.username,
            p.title, p.description, p.media_url, p.file_download_url, p.min_price, p.vendor_info,
            AVG(r.rating), COUNT(r.id)
        FROM digital_products p
        JOIN users u ON p.artist_id = u.id
        LEFT JOIN product_reviews r ON p.id = r.product_id
        WHERE p.id = ?
        GROUP BY p.id
        "#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    match row {
        Some(r) => Json(DigitalProductWithStats {
            id: r.0,
            artist_id: r.1,
            artist_name: r.2,
            artist_username: r.3,
            title: r.4,
            description: r.5,
            media_url: r.6,
            file_download_url: r.7,
            min_price: r.8,
            vendor_info: r.9,
            average_rating: r.10.unwrap_or(5.0),
            review_count: r.11,
            created_at: "".into(),
        }).into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "Product not found").into_response(),
    }
}

async fn create_product(
    State(state): State<AppState>,
    Json(payload): Json<CreateProductReq>,
) -> Response {
    let file_url = payload.file_download_url.unwrap_or_else(|| "https://example.com/asset_vault_download.zip".into());
    // Give each product a randomized curated discovery weight so home page isn't purely chronological
    let jitter_weight = 1.0 + (payload.title.len() % 5) as f64 * 0.15;

    let res = sqlx::query(
        r#"
        INSERT INTO digital_products (artist_id, title, description, media_url, file_download_url, min_price, vendor_info, score_weight)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#
    )
    .bind(payload.artist_id)
    .bind(&payload.title)
    .bind(&payload.description)
    .bind(&payload.media_url)
    .bind(&file_url)
    .bind(payload.min_price)
    .bind(&payload.vendor_info)
    .bind(jitter_weight)
    .execute(&state.db)
    .await;

    match res {
        Ok(r) => {
            let id = r.last_insert_rowid();
            let _ = state.tx.send(WsEvent {
                event_type: "PRODUCT_PUBLISHED".into(),
                message: format!("Artist launched new digital asset: '{}' (Min ${:.2})", payload.title, payload.min_price),
                payload: serde_json::json!({ "id": id, "title": payload.title, "price": payload.min_price }),
            });
            Json(serde_json::json!({ "id": id, "status": "created" })).into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn list_reviews(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Json<Vec<ProductReview>> {
    let reviews = sqlx::query_as::<_, ProductReview>(
        "SELECT id, product_id, user_id, user_name, rating, comment, created_at FROM product_reviews WHERE product_id = ? ORDER BY id DESC"
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Json(reviews)
}

async fn add_review(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<CreateReviewReq>,
) -> Response {
    let rating = payload.rating.clamp(1, 5);

    let user_name: Option<(String,)> = sqlx::query_as("SELECT display_name FROM users WHERE id = ?")
        .bind(payload.user_id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);

    let uname = user_name.map(|u| u.0).unwrap_or_else(|| "Collector".into());

    let res = sqlx::query(
        "INSERT INTO product_reviews (product_id, user_id, user_name, rating, comment) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(id)
    .bind(payload.user_id)
    .bind(&uname)
    .bind(rating)
    .bind(&payload.comment)
    .execute(&state.db)
    .await;

    match res {
        Ok(_) => {
            let _ = state.tx.send(WsEvent {
                event_type: "PRODUCT_REVIEW".into(),
                message: format!("New {}-star review submitted by {} for product #{}", rating, uname, id),
                payload: serde_json::json!({ "product_id": id, "rating": rating }),
            });
            Json(serde_json::json!({ "status": "ok" })).into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn purchase_product(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<PurchaseProductReq>,
) -> Response {
    let prod = sqlx::query_as::<_, DigitalProduct>("SELECT * FROM digital_products WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);

    let prod = match prod {
        Some(p) => p,
        None => return (axum::http::StatusCode::NOT_FOUND, "Product not found").into_response(),
    };

    // Client can pay whatever price they want, but NOT below min_price
    if payload.offered_price < prod.min_price {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Offered price (${:.2}) cannot be lower than the fixed base price (${:.2})", payload.offered_price, prod.min_price),
        ).into_response();
    }

    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    // 1. Record purchase
    let _ = sqlx::query(
        "INSERT INTO product_purchases (product_id, client_id, amount_paid) VALUES (?, ?, ?)"
    )
    .bind(id)
    .bind(payload.client_id)
    .bind(payload.offered_price)
    .execute(&mut *tx)
    .await;

    // 2. Direct ledger entry for digital product sale
    let _ = sqlx::query(
        "INSERT INTO escrow_ledger (commission_id, client_id, artist_id, amount, status) VALUES (NULL, ?, ?, ?, 'RELEASED_TO_ARTIST')"
    )
    .bind(payload.client_id)
    .bind(prod.artist_id)
    .bind(payload.offered_price)
    .execute(&mut *tx)
    .await;

    if let Err(e) = tx.commit().await {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let _ = state.tx.send(WsEvent {
        event_type: "PRODUCT_PURCHASED".into(),
        message: format!("Digital Product '{}' bought for ${:.2}!", prod.title, payload.offered_price),
        payload: serde_json::json!({ "product_id": id, "amount": payload.offered_price }),
    });

    Json(serde_json::json!({
        "status": "success",
        "download_url": prod.file_download_url,
        "amount_paid": payload.offered_price
    })).into_response()
}

async fn list_user_purchases(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let uid = match q.user_id {
        Some(id) => id,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id").into_response(),
    };

    let rows: Vec<(i64, i64, i64, f64, String, String, String)> = sqlx::query_as(
        r#"
        SELECT pu.id, pu.product_id, pu.client_id, pu.amount_paid, pr.title, pr.file_download_url, pu.purchased_at
        FROM product_purchases pu
        JOIN digital_products pr ON pu.product_id = pr.id
        WHERE pu.client_id = ?
        ORDER BY pu.id DESC
        "#,
    )
    .bind(uid)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let purchases: Vec<ProductPurchase> = rows
        .into_iter()
        .map(|r| ProductPurchase {
            id: r.0,
            product_id: r.1,
            client_id: r.2,
            amount_paid: r.3,
            product_title: r.4,
            file_download_url: r.5,
            purchased_at: r.6,
        })
        .collect();

    Json(purchases).into_response()
}

// ---------------------------------------------------------
// COMMISSIONS & ESCROW (Scoped for Client/Artist)
// ---------------------------------------------------------

async fn list_commissions(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(uid) => uid,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id").into_response(),
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

    let comms = if role == "Artist" {
        sqlx::query_as::<_, Commission>(
            "SELECT id, title, client_id, artist_id, status, price, brief, created_at, updated_at FROM commissions WHERE artist_id = ? ORDER BY id DESC"
        )
        .bind(user_id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
    } else {
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

    let _ = sqlx::query(
        "INSERT INTO escrow_ledger (commission_id, client_id, artist_id, amount, status) VALUES (?, ?, ?, ?, 'HELD_IN_ESCROW')"
    )
    .bind(comm_id)
    .bind(payload.client_id)
    .bind(payload.artist_id)
    .bind(payload.price)
    .execute(&mut *tx)
    .await;

    let _ = sqlx::query(
        "INSERT INTO commission_milestones (commission_id, stage_name, note) VALUES (?, 'REQUESTED', 'Brief launched by client and funds locked in safe escrow.')"
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
        "COMPLETED" => "Client approved all deliverables and settled the request.",
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

    let _ = sqlx::query("UPDATE commissions SET status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await;

    let _ = sqlx::query("UPDATE escrow_ledger SET status = 'RELEASED_TO_ARTIST' WHERE commission_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await;

    let cert_hash = format!(
        "CERT-{:x}-{}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        comm.client_id,
        id
    );

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

async fn list_escrow(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(uid) => uid,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id").into_response(),
    };

    let ledger = sqlx::query_as::<_, EscrowLedger>(
        "SELECT id, commission_id, client_id, artist_id, amount, status, created_at FROM escrow_ledger WHERE client_id = ? OR artist_id = ? ORDER BY id DESC"
    )
    .bind(user_id)
    .bind(user_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

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
        message: format!("Escrow entry #{} settled and released", id),
        payload: serde_json::json!({ "id": id }),
    });

    Json(serde_json::json!({ "status": "released" })).into_response()
}

async fn list_ownership(
    State(state): State<AppState>,
    Query(q): Query<UserQuery>,
) -> Response {
    let user_id = match q.user_id {
        Some(uid) => uid,
        None => return (axum::http::StatusCode::BAD_REQUEST, "Missing user_id").into_response(),
    };

    let logs = sqlx::query_as::<_, OwnershipLog>(
        "SELECT id, commission_id, character_name, owner_user_id, creator_user_id, license_type, certificate_hash, registered_at FROM ownership_logs WHERE owner_user_id = ? OR creator_user_id = ? ORDER BY id DESC"
    )
    .bind(user_id)
    .bind(user_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Json(logs).into_response()
}

// ---------------------------------------------------------
// WEBSOCKETS
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
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            tracing::debug!("Received ws text: {}", text);
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}
