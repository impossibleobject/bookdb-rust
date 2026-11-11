use anyhow::{Context, Result};
use dotenvy;
use std::{env, error::Error, net::SocketAddr, sync::Arc};
use tokio_postgres::{Client, NoTls};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Html,
    routing::{get, put},
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

mod migrate_database;

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

// Shared application state
type Db = Arc<Client>;

//corresponding to book table entries
#[derive(Debug, Serialize)]
struct Book {
    id: i64,
    source_bookpedia_id: i64,
    title: Option<String>,
    isbn: Option<String>,
    publisher: Option<String>,
    author_text: Option<String>,
    release_date: Option<NaiveDate>,
    date_added: Option<NaiveDate>,
    extra_fields: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct BookInput {
    source_bookpedia_id: Option<i64>,
    title: Option<String>,
    isbn: Option<String>,
    publisher: Option<String>,
    author_text: Option<String>,
    release_date: Option<NaiveDate>,
    date_added: Option<NaiveDate>,
    extra_fields: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: Option<String>,
}

// Serve browser interface
async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

// Retrieve books, optionally filtered by title or author
async fn list_books(
    State(db): State<Db>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<Vec<Book>>, StatusCode> {
    let search = format!("%{}%", params.q.unwrap_or_default());

    let rows = db
        .query(
            "
        SELECT
            id,
            source_bookpedia_id,
            title,
            isbn,
            publisher,
            author_text,
            release_date,
            date_added,
            extra_fields
        FROM books
        WHERE COALESCE(title, '') ILIKE $1
           OR COALESCE(author_text, '') ILIKE $1
        ORDER BY title ASC
        LIMIT 500
        ",
            &[&search],
        )
        .await
        .map_err(|e| {
            eprintln!("Database error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let books = rows
        .iter()
        .map(|row| Book {
            id: row.get("id"),
            source_bookpedia_id: row.get("source_bookpedia_id"),
            title: row.get("title"),
            isbn: row.get("isbn"),
            publisher: row.get("publisher"),
            author_text: row.get("author_text"),
            release_date: row.get("release_date"),
            date_added: row.get("date_added"),
            extra_fields: row.get("extra_fields"),
        })
        .collect();

    Ok(Json(books))
}

//add book to db via browser
async fn create_book(
    State(db): State<Db>,
    Json(book): Json<BookInput>,
) -> Result<Json<Book>, StatusCode> {
    let title = book
        .title
        .filter(|t| !t.trim().is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;

    let source_id = book.source_bookpedia_id.ok_or(StatusCode::BAD_REQUEST)?;

    let extra_fields = book.extra_fields.unwrap_or_else(|| serde_json::json!({}));

    let row = db
        .query_one(
            "
        INSERT INTO books (
            source_bookpedia_id,
            title,
            isbn,
            publisher,
            author_text,
            release_date,
            date_added,
            extra_fields
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING
            id,
            source_bookpedia_id,
            title,
            isbn,
            publisher,
            author_text,
            release_date,
            date_added,
            extra_fields
        ",
            &[
                &source_id,
                &title,
                &book.isbn,
                &book.publisher,
                &book.author_text,
                &book.release_date,
                &book.date_added,
                &extra_fields,
            ],
        )
        .await
        .map_err(|e| {
            eprintln!("Insert error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(Book {
        id: row.get("id"),
        source_bookpedia_id: row.get("source_bookpedia_id"),
        title: row.get("title"),
        isbn: row.get("isbn"),
        publisher: row.get("publisher"),
        author_text: row.get("author_text"),
        release_date: row.get("release_date"),
        date_added: row.get("date_added"),
        extra_fields: row.get("extra_fields"),
    }))
}

// Update existing book via browser
async fn update_book(
    State(db): State<Db>,
    Path(id): Path<i64>,
    Json(book): Json<BookInput>,
) -> Result<Json<Book>, StatusCode> {
    let title = book
        .title
        .filter(|t| !t.trim().is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;

    let extra_fields = book.extra_fields.unwrap_or_else(|| serde_json::json!({}));

    let row = db
        .query_opt(
            "
        UPDATE books
        SET
            title = $1,
            isbn = $2,
            publisher = $3,
            author_text = $4,
            release_date = $5,
            date_added = $6,
            extra_fields = $7
        WHERE id = $8
        RETURNING
            id,
            source_bookpedia_id,
            title,
            isbn,
            publisher,
            author_text,
            release_date,
            date_added,
            extra_fields
        ",
            &[
                &title,
                &book.isbn,
                &book.publisher,
                &book.author_text,
                &book.release_date,
                &book.date_added,
                &extra_fields,
                &id,
            ],
        )
        .await
        .map_err(|e| {
            eprintln!("Update error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let row = row.ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(Book {
        id: row.get("id"),
        source_bookpedia_id: row.get("source_bookpedia_id"),
        title: row.get("title"),
        isbn: row.get("isbn"),
        publisher: row.get("publisher"),
        author_text: row.get("author_text"),
        release_date: row.get("release_date"),
        date_added: row.get("date_added"),
        extra_fields: row.get("extra_fields"),
    }))
}

// execute sql query in browser
async fn execute_query(
    State(db): State<Db>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<Value>, axum::http::StatusCode> {
    let sql = request.sql.trim();

    if sql.is_empty() {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    // Execute query and aggregate results into JSON
    let result = db
        .query_one(
            &format!(
                "SELECT COALESCE(json_agg(q), '[]'::json) \
                 FROM ({}) AS q",
                sql
            ),
            &[],
        )
        .await
        .map_err(|e| {
            eprintln!("SQL error: {}", e);
            axum::http::StatusCode::BAD_REQUEST
        })?;

    let json: Value = result.get(0);

    Ok(Json(json))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // migrate and verify database from sqlite "Database.bookpd" file (READ ONLY access)
    //to existing postgres database accessed via URL in .env
    dotenvy::dotenv().ok();
    //get postgres url
    let postgres_url = env::var("POSTGRES_URL")
        .context("POSTGRES_URL is missing; create .env")
        .expect("Release")
        .to_string();

    // run migration of sqlite into postgres
    // let mut target: Client = Client::connect(&postgres_url, NoTls)
    //     .context("connecting to PostgreSQL; create the target database first").expect("Release");

    // let bookpd_path = env::var("BOOKPD_PATH").context("BOOKPD_PATH is missing; create .env").expect("Release").to_string();
    // let source = migrate_database::open_sqlite(&bookpd_path).expect("Release");

    // let _ = migrate_database::migrate(&source, &mut target);
    // let _ = migrate_database::verify(&source, &mut target);

    // run browser interface on localhost
    // connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&postgres_url, NoTls).await?;
    // Keep the PostgreSQL connection alive
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });
    let db = Arc::new(client);

    // Configure routes
    let app = Router::new()
        .route("/", get(index))
        .route("/api/books", get(list_books).post(create_book))
        .route("/api/books/{id}", put(update_book))
        .nest_service("/covers", ServeDir::new("covers"))
        .with_state(db);

    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let listener = TcpListener::bind(addr).await?;

    println!("Bookpedia running at http://localhost:3000");

    axum::serve(listener, app).await?;

    Ok(())
}
