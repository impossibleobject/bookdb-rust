
use std::env;
use dotenvy;
use anyhow::{Context};
use postgres::{Client, NoTls};

mod migrate_database;

fn main(){
    // migrate and verify database from sqlite "Database.bookpd" file (READ ONLY access)
    //to existing postgres database accessed via URL in .env
    dotenvy::dotenv().ok();
    let postgres_url = env::var("POSTGRES_URL").context("POSTGRES_URL is missing; create .env").expect("Release").to_string();
    let bookpd_path = env::var("BOOKPD_PATH").context("POSTGRES_URL is missing; create .env").expect("Release").to_string();

    let source = migrate_database::open_sqlite(&bookpd_path).expect("Release");
    let mut target: Client = Client::connect(&postgres_url, NoTls)
        .context("connecting to PostgreSQL; create the target database first").expect("Release");
    
    let _ = migrate_database::migrate(&source, &mut target);
    let _ = migrate_database::verify(&source, &mut target);
}