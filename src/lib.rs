use futures_util::stream::StreamExt;
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Query, State},
    response::{IntoResponse, Response, Json},
    routing::{any, get}, 
    Router,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite, FromRow};
use tokio::task::JoinHandle;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct AppState {
    db: Arc<Pool<Sqlite>>,
}

#[derive(Debug, Deserialize)]
struct HashQuery {
    block_id: u64, // The block ID from the quary string
}

#[derive(Serialize)]
struct HashOnly {
    hash: String,
}

/// Simulated blockchain block.
#[derive(Debug, Serialize, Deserialize, FromRow)]
struct Block {                     
    index_id: u64,                 // position in the chain
    timestamp: u64,                // time when block is made 
    transactions: String,          // transactions
    nonce: u64,                    // used for proof-of-work
    hash: String,                  // hash which will be caluculated
    previous_hash: String,         // hash of previous block
}

pub async fn blockchain_client_http_server_main () -> JoinHandle<()> {
    std::fs::create_dir_all("./data").expect("Creating `data` should not fail");

    let db_path = std::env::current_dir().unwrap().join("data/blockchain.db");
    tracing::debug!("Attempting to connect to database at: {}", db_path.display());

    let db = SqlitePoolOptions::new()
        .connect(db_path.to_str().unwrap())
        .await
        .expect("Failed to connect to SQLite");

    // Create table if it doesn't exist
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            index_id INTEGER,            
            timestamp INTEGER,
            transactions TEXT,
            nonce INTEGER,
            hash TEXT,
            previous_hash TEXT
        )
        "#,
    )
    .execute(&db)
    .await
    .unwrap();

    let app_state = AppState{
        db: Arc::new(db),
    };

    let app = Router::new()
        .route("/ws", any(handler_websocket))
        .route("/hash", get(handler_http))
        .with_state(app_state.clone());

    let addr = "127.0.0.1:7878".parse().unwrap();
    tracing::info!("blockchain_client_server listening on ws://{}", addr);

    let server_handle = tokio::spawn(async move {
        axum_server::Server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    });

    server_handle
}

async fn handler_websocket(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))    
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (_, mut receiver) = socket.split();

    tracing::info!("WebSocket connection established!");

    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            tracing::debug!(%text, "Raw message");
            let bytes = text.as_bytes();
            tracing::debug!(?bytes, "Raw message");
            match serde_json::from_str::<Block>(&text) {
                Ok(block) => {
                    tracing::debug!(?block, "Received block");

                    if let Err(err) = sqlx::query(
                        r#"
                        INSERT INTO blocks (index_id, timestamp, transactions, nonce, hash, previous_hash)
                        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                        "#,
                    )
                    .bind(block.index_id.try_into().expect("Conversion error"))
                    .bind(block.timestamp as i64)
                    .bind(block.transactions)
                    .bind(block.nonce as i64)
                    .bind(block.hash)
                    .bind(block.previous_hash)
                    .execute(state.db.as_ref())
                    .await
                    {
                        tracing::info!("DB insert error: {}", err);
                    }
                },
                Err(e) => tracing::info!("Failed to parse block {e}"),
            }
        }
    }
}

async fn handler_http (Query(params): Query<HashQuery>, 
                       State(state): State<AppState>) -> impl IntoResponse {

    // Query the database using the block_id
    let result: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        r#"
        SELECT
            hash
        FROM blocks
        WHERE index_id = ?
        "#
    )
    .bind(params.block_id as i64)
    .fetch_optional(&*state.db)
    .await;

    match result {
        Ok(Some(hash)) => Json(HashOnly{hash}).into_response(), // 200 OK with JSON
        Ok(None) => (StatusCode::NOT_FOUND, "Block not found").into_response(), // 404
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
            .into_response(), // 500
    }
}
