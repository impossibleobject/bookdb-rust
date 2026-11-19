use anyhow::{Context, Result};
use dotenvy;
use std::{env, error::Error, net::SocketAddr, sync::Arc};
use tokio_postgres::{Client, NoTls};
use postgres;

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
    bookpedia_id: i64,
    title: Option<String>,
    author: Option<String>,
    publishing_house: Option<String>,
    series: Option<String>,
    edition: Option<String>,
    format: Option<String>,
    release_date: Option<NaiveDate>,
    publisher: Option<String>,
    genre: Option<String>,
    pages: Option<i32>,
    isbn: Option<String>,
    topics: Option<String>,
    comments: Option<String>,
    summary: Option<String>,
    extra_fields: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct BookInput {
    bookpedia_id: Option<i64>,
    title: Option<String>,
    author: Option<String>,
    publishing_house: Option<String>,
    series: Option<String>,
    edition: Option<String>,
    format: Option<String>,
    release_date: Option<NaiveDate>,
    publisher: Option<String>,
    genre: Option<String>,
    pages: Option<i32>,
    isbn: Option<String>,
    topics: Option<String>,
    comments: Option<String>,
    summary: Option<String>,
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
            bookpedia_id,
            title,
            author,
            publishing_house,
            series,
            edition,
            format,
            release_date,
            publisher,
            genre,
            pages,
            isbn,
            topics,
            comments,
            summary,
            extra_fields
        FROM books
        WHERE COALESCE(title, '') ILIKE $1
           OR COALESCE(author, '') ILIKE $1
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
            bookpedia_id: row.get("bookpedia_id"),
            title: row.get("title"),
            author: row.get("author"),
            publishing_house: row.get("publishing_house"),
            series: row.get("series"),
            edition: row.get("edition"),
            format: row.get("format"),
            release_date: row.get("release_date"),
            publisher: row.get("publisher"),
            genre: row.get("genre"),
            pages: row.get("pages"),
            isbn: row.get("isbn"),
            topics: row.get("topics"),
            comments: row.get("comments"),
            summary: row.get("summary"),
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

    let bookpedia_id = book.bookpedia_id.ok_or(StatusCode::BAD_REQUEST)?;

    let extra_fields = book.extra_fields.unwrap_or_else(|| serde_json::json!({}));

    let row = db
        .query_one(
            "
        INSERT INTO books (
            bookpedia_id,
            title,
            author,
            publishing_house,
            series,
            edition,
            format,
            release_date,
            publisher,
            genre,
            pages,
            isbn,
            topics,
            comments,
            summary,
            extra_fields
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        RETURNING
            id,
            bookpedia_id,
            title,
            author,
            publishing_house,
            series,
            edition,
            format,
            release_date,
            publisher,
            genre,
            pages,
            isbn,
            topics,
            comments,
            summary,
            extra_fields
        ",
            &[
                &bookpedia_id,
                &title,
                &book.author,
                &book.publishing_house,
                &book.series,
                &book.edition,
                &book.format,
                &book.release_date,
                &book.publisher,
                &book.genre,
                &book.pages,
                &book.isbn,
                &book.topics,
                &book.comments,
                &book.summary,
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
        bookpedia_id: row.get("bookpedia_id"),
        title: row.get("title"),
        author: row.get("author"),
        publishing_house: row.get("publishing_house"),
        series: row.get("series"),
        edition: row.get("edition"),
        format: row.get("format"),
        release_date: row.get("release_date"),
        publisher: row.get("publisher"),
        genre: row.get("genre"),
        pages: row.get("pages"),
        isbn: row.get("isbn"),
        topics: row.get("topics"),
        comments: row.get("comments"),
        summary: row.get("summary"),
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
            author = $2,
            publishing_house = $3,
            series = $4,
            edition = $5,
            format = $6,
            release_date = $7,
            publisher = $8,
            genre = $9,
            pages = $10,
            isbn = $11,
            topics = $12,
            comments = $13,
            summary = $14,
            extra_fields = $15
        WHERE id = $16
        RETURNING
            id,
            bookpedia_id,
            title,
            author,
            publishing_house,
            series,
            edition,
            format,
            release_date,
            publisher,
            genre,
            pages,
            isbn,
            topics,
            comments,
            summary,
            extra_fields
        ",
            &[
                &title,
                &book.author,
                &book.publishing_house,
                &book.series,
                &book.edition,
                &book.format,
                &book.release_date,
                &book.publisher,
                &book.genre,
                &book.pages,
                &book.isbn,
                &book.topics,
                &book.comments,
                &book.summary,
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
        bookpedia_id: row.get("bookpedia_id"),
        title: row.get("title"),
        author: row.get("author"),
        publishing_house: row.get("publishing_house"),
        series: row.get("series"),
        edition: row.get("edition"),
        format: row.get("format"),
        release_date: row.get("release_date"),
        publisher: row.get("publisher"),
        genre: row.get("genre"),
        pages: row.get("pages"),
        isbn: row.get("isbn"),
        topics: row.get("topics"),
        comments: row.get("comments"),
        summary: row.get("summary"),
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

#[tokio::main] //
async fn main() -> Result<(), Box<dyn Error>> {
    // migrate and verify database from sqlite "Database.bookpd" file (READ ONLY access)
    //to existing postgres database accessed via URL in .env
    dotenvy::dotenv().ok();
    //get postgres url
    let postgres_url = env::var("POSTGRES_URL")
        .context("POSTGRES_URL is missing; create .env")
        .expect("Release")
        .to_string();



    // // run migration of sqlite into postgres
    // let mut target: postgres::Client = postgres::Client::connect(&postgres_url, NoTls)
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
