# bookdb-rust
An attempt at reimplementation of some basic features of the Mac Tool Bookpedia from Bruji 
https://www.bruji.com/bookpedia/
using Postgresql and Rust for personal use


# NOT READY FOR USE
At the moment only basic localhost browser interface and Migration Utility

# Setup Requirements:
1. Setup local Postgresql server

2. repo/.env with
POSTGRES_URL="URL with password and address of local postgresql database, postgresql://"
BOOKPD_PATH="PATH to Database.bookpd file, on MAC in Library/Application Support"

3. folder repo/covers for book covers also from bookpedia numbered after ZUID


# Features:
Migrate existing Sqlite bookpedia Database using synchronious mode (no tokio) 

Run local browser interface in tokio mode:
add pr edit entries to database in browser such that they appear in the database
Search using Title and Author
Show book Covers

