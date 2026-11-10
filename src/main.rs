use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    error::Error,
};
use dotenvy;
use anyhow::{Context, Result};
use tokio_postgres::{NoTls, Client};

use axum::{
    extract::State,
    routing::{get, post},
    Json,
    Router,
};
use tokio::{net::TcpListener};
use serde::Deserialize;
use serde_json::Value;




#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

// Shared application state
type Db = Arc<Client>;



//mod migrate_database;




// --------------------------------------------------
// Browser interface
// --------------------------------------------------
async fn index() -> axum::response::Html<&'static str> {
    axum::response::Html(r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>PostgreSQL Browser</title>

        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 40px;
            }

            textarea {
                width: 100%;
                height: 180px;
                font-family: monospace;
                font-size: 14px;
                padding: 10px;
                box-sizing: border-box;
            }

            button {
                margin-top: 10px;
                padding: 10px 20px;
                cursor: pointer;
            }

            pre {
                background: #eee;
                padding: 15px;
                white-space: pre-wrap;
                overflow-x: auto;
            }
        </style>
    </head>

    <body>

        <h1>PostgreSQL Browser</h1>

        <h3>SQL Query</h3>

        <textarea id="sql">
SELECT * FROM books LIMIT 20;
        </textarea>

        <br>

        <button onclick="runQuery()">
            Execute SQL
        </button>

        <h3>Result</h3>

        <pre id="result">Waiting for query...</pre>

        <script>
            async function runQuery() {

                const sql = document.getElementById("sql").value;
                const output = document.getElementById("result");

                output.textContent = "Executing...";

                try {

                    const response = await fetch("/api/query", {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json"
                        },
                        body: JSON.stringify({ sql: sql })
                    });

                    const data = await response.json();

                    output.textContent =
                        JSON.stringify(data, null, 2);

                } catch (error) {
                    output.textContent = "Error: " + error.message;
                }
            }
        </script>

    </body>
    </html>
    "#)
}



// --------------------------------------------------
// SQL query endpoint
// --------------------------------------------------
// 
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
async fn main()-> Result<(), Box<dyn Error>>{
    // migrate and verify database from sqlite "Database.bookpd" file (READ ONLY access)
    //to existing postgres database accessed via URL in .env
    dotenvy::dotenv().ok();
    //get postgres url
    let postgres_url = env::var("POSTGRES_URL").context("POSTGRES_URL is missing; create .env").expect("Release").to_string();


    // run migration of sqlite into postgres
    // let mut target: Client = Client::connect(&postgres_url, NoTls)
    //     .context("connecting to PostgreSQL; create the target database first").expect("Release");

    // let bookpd_path = env::var("BOOKPD_PATH").context("BOOKPD_PATH is missing; create .env").expect("Release").to_string();
    // let source = migrate_database::open_sqlite(&bookpd_path).expect("Release");
    
    // let _ = migrate_database::migrate(&source, &mut target);
    // let _ = migrate_database::verify(&source, &mut target);
    


    // run browser interface on localhost
    let (client, connection) =
        tokio_postgres::connect(&postgres_url, NoTls).await?;

    // Keep the PostgreSQL connection alive
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });
    let db = Arc::new(client);


    let app = Router::new()
        .route("/", get(index))
        .route("/api/query", post(execute_query))
        .with_state(db);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Listening on: {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())

}



    

    