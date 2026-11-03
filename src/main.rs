use axum::{
    routing::{get, post},
    extract::{State, Json, Path},
    http::StatusCode,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Serialize, Deserialize)]
pub struct CommissionSlot {
    id: String,
    artist_id: String,
    title: String,
    description: String,
    price: f64,
    product_type: String, // "digital" or "physical"
    is_active: bool,
    slots_available: i32,
}

#[derive(Deserialize)]
pub struct CreateSlotRequest {
    artist_id: String,
    title: String,
    description: String,
    price: f64,
    product_type: String, // Can be "digital" or "physical"
    slots_available: i32,
}

struct AppState {
    slots: Mutex<Vec<CommissionSlot>>,
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState {
        slots: Mutex::new(vec![]),
    });

    let app = Router::new()
        .route("/api/slots", get(get_slots).post(create_slot))
        .route("/api/slots/:id", post(update_slot))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("AtelierV server running on http://127.0.0.1:8080");
    axum::serve(listener, app).await.unwrap();
}

async fn get_slots(State(state): State<Arc<AppState>>) -> Json<Vec<CommissionSlot>> {
    let slots = state.slots.lock().unwrap();
    Json(slots.clone())
}

async fn create_slot(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateSlotRequest>,
) -> Result<(StatusCode, Json<CommissionSlot>), StatusCode> {
    // FIX APPLIED: Artists can now add commission slots for both digital and physical products.
    // Previously, digital-only artists were blocked by a physical inventory check.
    // We now explicitly allow product_type == "digital" as well as "physical".

    if payload.title.is_empty() || payload.price < 0.0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let new_slot = CommissionSlot {
        id: uuid::Uuid::new_v4().to_string(),
        artist_id: payload.artist_id,
        title: payload.title,
        description: payload.description,
        price: payload.price,
        product_type: payload.product_type,
        is_active: true,
        slots_available: payload.slots_available,
    };

    let mut slots = state.slots.lock().unwrap();
    slots.push(new_slot.clone());

    Ok((StatusCode::CREATED, Json(new_slot)))
}

async fn update_slot(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateSlotRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut slots = state.slots.lock().unwrap();
    if let Some(slot) = slots.iter_mut().find(|s| s.id == id) {
        slot.title = payload.title;
        slot.description = payload.description;
        slot.price = payload.price;
        slot.product_type = payload.product_type;
        slot.slots_available = payload.slots_available;
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
