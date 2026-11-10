use axum::{routing::get, Router};

#[tokio::main]
async fn main() {
  
  let app = Router::new().route("/", get(|| async {"hell world"}));

  let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
  println!("server listen on port : {}", listener.local_addr().unwrap());
  axum::serve(listener, app).await.unwrap();
}
