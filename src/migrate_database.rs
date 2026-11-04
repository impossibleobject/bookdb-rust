//! Read-only SQLite extraction for bookpedia files, clean PostgreSQL import, and verification.
//!
//! The normalized tables are intended for application use. `source_archive` retains
//! every original table row and column, including Core Data metadata and custom fields.

use anyhow::{Context, Ok, Result, bail};
use postgres::Client;
use rusqlite::{types::Value, Connection};
use chrono::{DateTime, NaiveDate, Utc};

/// converts dates from coredata/sqlite to postgres
fn coredata_to_postgres_date(value: Option<String>) -> Option<NaiveDate> {
    let timestamp: f64 = value.as_deref()?.parse().ok()?;

    // Apple's Core Data epoch: 2001-01-01 00:00:00 UTC
    let apple_epoch = DateTime::parse_from_rfc3339("2001-01-01T00:00:00+00:00")
        .ok()?
        .with_timezone(&Utc);

    let datetime = apple_epoch
        + chrono::Duration::milliseconds((timestamp * 1000.0).round() as i64);

    Some(datetime.date_naive())
}

fn to_i32(value: Option<i64>) -> Option<i32> {
    value.and_then(|v| i32::try_from(v).ok())
}

/// shows postgres errors on commandline, skipped explicit handling
fn print_postgres_error(
    result: Result<u64, postgres::Error>,
    source_id: Option<i64>,
) {
    if let Err(e) = result {
        eprintln!("ERROR importing Bookpedia record {:?}", source_id);
        eprintln!("PostgreSQL error: {e}");
        eprintln!("Full error details: {e:?}");
        std::process::abort();
    }
}

pub fn open_sqlite(path: &str) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    ).with_context(|| format!("opening SQLite source {path}"))?)
}

/// formats table and column names for sql queries
fn qi(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// extracts table names in alphabetical ordered
pub fn table_names(src: &Connection) -> Result<Vec<String>> {
    let mut stmt = src.prepare(
        "SELECT name FROM sqlite_master
         WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn row_count(src: &Connection, table: &str) -> Result<i64> {
    Ok(src.query_row(&format!("SELECT COUNT(*) FROM {}", qi(table)), [], |r| r.get(0))?)
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    declared_type: String,
    pk_order: i64,
}

/// get vector of rusqlite columns from a specific table
fn columns(
    src: &rusqlite::Connection,
    table: &str,
) -> anyhow::Result<Vec<Column>> {
    let sql = format!("PRAGMA table_info({})", qi(table));
    let mut stmt = src.prepare(&sql)?;

    let rows = stmt.query_map([], |r| {
        let name: String = r.get(1)?;
        let declared_type: String = r.get::<_, Option<String>>(2)?.unwrap_or_default();
        let pk_order: i64 = r.get(5)?;

        rusqlite::Result::Ok(Column {
            name,
            declared_type,
            pk_order,
        })
    })?;

    let columns = rows.collect::<rusqlite::Result<Vec<Column>>>()?;

    anyhow::Result::Ok(columns)
}

/// reformat json strings
fn json_escape(s: &str) -> String {
    let mut out: String = String::new();
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Store both value and SQLite storage class so the archive remains interpretable.
fn json_value(v: &Value) -> String {
    match v {
        Value::Null => r#"{"sqlite_type":"null","value":null}"#.into(),
        Value::Integer(n) => format!(r#"{{"sqlite_type":"integer","value":{n}}}"#),
        Value::Real(n) => format!(r#"{{"sqlite_type":"real","value":{n}}}"#),
        Value::Text(s) => format!(
            r#"{{"sqlite_type":"text","value":"{}"}}"#, json_escape(s)
        ),
        Value::Blob(b) => format!(
            r#"{{"sqlite_type":"blob","hex":"{}","length":{}}}"#, hex(b), b.len()
        ),
    }
}

/// get (primary) key for table from rusqlite format
fn source_key(cols: &[Column], values: &[Value], ordinal: usize) -> String {
    let preferred = cols.iter().position(|c| c.name == "Z_PK")
        .or_else(|| cols.iter().position(|c| c.pk_order > 0));
    if let Some(i) = preferred {
        match &values[i] {
            Value::Integer(n) => return n.to_string(),
            Value::Text(s) => return s.clone(),
            _ => {}
        }
    }
    // Tables such as Z_2COLLECTIONS have no declared primary key.
    format!("row-{ordinal:012}")
}

/// convert non-included rows into accessible json entry
fn row_json(cols: &[Column], values: &[Value]) -> serde_json::Value {
    //keep types while avoiding the warning, cheeky, TODO
    let _types = cols.iter().zip(values).map(|(c, v)| {
        format!(r#""{}":{}"#, json_escape(&c.declared_type), json_value(v))
    }).collect::<Vec<_>>();
    let fields = cols.iter().zip(values).map(|(c, v)| {
        format!(r#""{}":{}"#, json_escape(&c.name), json_value(v))
    }).collect::<Vec<_>>();
    let str_json: String = format!("{{{}}}", fields.join(","));
    serde_json::from_str::<serde_json::Value>(str_json.as_str()).expect("Reason") //skip error handling
}

/// extract table as vector of columns and matrix of values
fn read_table(src: &Connection, table: &str) -> Result<(Vec<Column>, Vec<Vec<Value>>)> {
    let cols = columns(src, table)?;
    let names = cols.iter().map(|c| qi(&c.name)).collect::<Vec<_>>().join(", ");
    let mut stmt = src.prepare(&format!("SELECT {names} FROM {}", qi(table)))?;
    let mut rows = stmt.query([])?;
    let mut output = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = Vec::new();
        for i in 0..cols.len() {
            values.push(row.get::<_, Value>(i)?);
        }
        output.push(values);
    }
    Ok((cols, output))
}

/// basic postgres db (target) schema is defined here
pub fn create_schema(dst: &mut Client) -> Result<()> {
    dst.batch_execute(r#"
        CREATE TABLE IF NOT EXISTS books (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_bookpedia_id BIGINT NOT NULL UNIQUE,
            title TEXT,
            isbn TEXT,
            publisher TEXT,
            author_text TEXT,
            release_date DATE,
            date_added DATE,
            extra_fields JSONB NOT NULL DEFAULT '{}'::jsonb,
            extra_data BYTEA
        );

        CREATE TABLE IF NOT EXISTS collections (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_bookpedia_id BIGINT NOT NULL UNIQUE,
            name TEXT,
            collection_type INTEGER,
            view_type INTEGER,
            parent_source_id BIGINT,
            url TEXT,
            extra_data BYTEA,
            sort_descriptors BYTEA,
            visible_columns BYTEA,
            predicate_data BYTEA
        );

        CREATE TABLE IF NOT EXISTS book_collections (
            book_id BIGINT NOT NULL REFERENCES books(id) ON DELETE CASCADE,
            collection_id BIGINT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
            PRIMARY KEY (book_id, collection_id)
        );

        CREATE TABLE IF NOT EXISTS book_links (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_bookpedia_id BIGINT UNIQUE,
            book_id BIGINT REFERENCES books(id) ON DELETE CASCADE,
            position INTEGER,
            name TEXT,
            link_type TEXT,
            url TEXT,
            related_entry_id BIGINT
        );

        CREATE TABLE IF NOT EXISTS loan_history (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_bookpedia_id BIGINT UNIQUE,
            book_source_id BIGINT,
            related_entry_id BIGINT,
            borrower_name TEXT,
            loan_date DATE,
            returned_at DATE
        );

        CREATE TABLE IF NOT EXISTS recently_used_entries (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            source_bookpedia_id BIGINT UNIQUE,
            book_source_id BIGINT,
            related_entry_id BIGINT,
            used_at DATE,
            name TEXT
        );

        -- Loss-preservation layer: all original tables/columns/values go here.
        CREATE TABLE IF NOT EXISTS source_archive (
            source_table TEXT NOT NULL,
            source_key TEXT NOT NULL,
            row_data JSONB NOT NULL,
            PRIMARY KEY (source_table, source_key)
        );

        CREATE INDEX IF NOT EXISTS books_title_idx ON books(title);
        CREATE INDEX IF NOT EXISTS books_isbn_idx ON books(isbn);
    "#)?;
    Ok(())
}

/// series of function to extract value from specific table
fn text_at(cols: &[Column], row: &[Value], name: &str) -> Option<String> {
    let i = cols.iter().position(|c| c.name == name)?;
    match &row[i] {
        Value::Null => None,
        Value::Integer(n) => Some(n.to_string()),
        Value::Real(n) => Some(n.to_string()),
        Value::Text(s) => Some(s.clone()),
        Value::Blob(b) => Some(format!("[BLOB {} bytes]", b.len())),
    }
}

fn int_at(cols: &[Column], row: &[Value], name: &str) -> Option<i64> {
    let i = cols.iter().position(|c| c.name == name)?;
    match &row[i] {
        Value::Integer(n) => Some(*n),
        Value::Real(n) => Some(*n as i64),
        Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

fn blob_at(cols: &[Column], row: &[Value], name: &str) -> Option<Vec<u8>> {
    let i = cols.iter().position(|c| c.name == name)?;
    match &row[i] {
        Value::Blob(b) => Some(b.clone()),
        _ => None,
    }
}

/// unused so far
// fn bool_at(cols: &[Column], row: &[Value], name: &str) -> Option<bool> {
//     int_at(cols, row, name).map(|n| n != 0)
// }

/// Import known Bookpedia fields to readable tables; unknown fields remain in JSONB
/// and all original fields also remain in `source_archive`.
fn import_books(src: &Connection, dst: &mut Client) -> Result<()> {
    let names = table_names(src)?;
    if !names.iter().any(|n| n == "ZENTRY") { return Ok(()); }
    let (cols, rows) = read_table(src, "ZENTRY")?;
    let mut tx = dst.transaction()?;
    //println!("starting to go through rows {:?}", rows.iter().take(5));
    //println!("starting to go through cols {:?}", cols.iter().take(5));

    for row in &rows {
        let source_id = int_at(&cols, row, "Z_PK")
            .context("ZENTRY row has no integer Z_PK")?;
        //TODO: left out for now 
        // Keep every source attribute, tagged with its original SQLite storage class.
        // let promoted = [
        //     "Z_PK", "ZTITLE", "ZISBN", "ZPUBLISHER", "ZAUTHOR", "ZRELEASEDATE",
        //     "ZDATEADDED", "ZEXTRADATA",
        // ];
        // let extra = cols.iter().zip(row).filter(|(c, _)| !promoted.contains(&c.name.as_str()))
        //     .map(|(c, v)| format!(r#""{}":{}"#, json_escape(&c.name), json_value(v)))
        //     .collect::<Vec<_>>().join(",");
        let extra_json = serde_json::from_str::<serde_json::Value>("{}")?;
        
        
        let result: std::prelude::v1::Result<u64, postgres::Error> = tx.execute(
            r#"INSERT INTO books
               (source_bookpedia_id,title,isbn,publisher,author_text,release_date,
                date_added,extra_fields,extra_data)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb, $9)
               ON CONFLICT (source_bookpedia_id) DO UPDATE SET
                 title=EXCLUDED.title, isbn=EXCLUDED.isbn, publisher=EXCLUDED.publisher,
                 author_text=EXCLUDED.author_text, release_date=EXCLUDED.release_date,
                 date_added=EXCLUDED.date_added, extra_fields=EXCLUDED.extra_fields,
                 extra_data=EXCLUDED.extra_data"#,
            &[
                &source_id,
                &text_at(&cols,row,"ZTITLE"),
                &text_at(&cols,row,"ZISBN"),
                &text_at(&cols,row,"ZPUBLISHER"),
                &text_at(&cols,row,"ZAUTHOR"),
                &coredata_to_postgres_date(text_at(&cols,row,"ZRELEASEDATE")),
                &coredata_to_postgres_date(text_at(&cols,row,"ZDATEADDED")),
                &extra_json,
                &blob_at(&cols,row,"ZEXTRADATA"),
            ],
        );
        let source_id_option:Option<i64> = Some(source_id);
        print_postgres_error(result, source_id_option);
        //println!("finished row {}", source_id);
    }
    tx.commit()?;
    println!("Imported {} books", rows.len());
    Ok(())
}

fn import_collections(src: &Connection, dst: &mut Client) -> Result<()> {
    let names = table_names(src)?;
    if !names.iter().any(|n| n == "ZCOLLECTION") { return Ok(()); }
    let (cols, rows) = read_table(src, "ZCOLLECTION")?;
    let mut tx = dst.transaction()?;
    for row in &rows {
        let source_id = int_at(&cols,row,"Z_PK").context("ZCOLLECTION row missing Z_PK")?;
        let result = tx.execute(
            r#"INSERT INTO collections
               (source_bookpedia_id,name,collection_type,view_type,parent_source_id,url,
                extra_data,sort_descriptors,visible_columns,predicate_data)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
               ON CONFLICT (source_bookpedia_id) DO UPDATE SET
                name=EXCLUDED.name, collection_type=EXCLUDED.collection_type,
                view_type=EXCLUDED.view_type, parent_source_id=EXCLUDED.parent_source_id,
                url=EXCLUDED.url, extra_data=EXCLUDED.extra_data,
                sort_descriptors=EXCLUDED.sort_descriptors,
                visible_columns=EXCLUDED.visible_columns, predicate_data=EXCLUDED.predicate_data"#,
            &[
                &source_id, &text_at(&cols,row,"ZNAME"), &to_i32(int_at(&cols,row,"ZTYPE")),
                &to_i32(int_at(&cols,row,"ZVIEWTYPE")), &int_at(&cols,row,"ZPARENT"),
                &text_at(&cols,row,"ZURL"), &blob_at(&cols,row,"ZEXTRADATA"),
                &blob_at(&cols,row,"ZSORTDESCRIPTORS"), &blob_at(&cols,row,"ZCOLUMNS"),
                &blob_at(&cols,row,"ZPREDICATEDATA"),
            ],
        );
        let option_source_id: Option<i64> = Some(source_id);
        print_postgres_error(result, option_source_id);
    }
    tx.commit()?;
    println!("Imported {} collections", rows.len());
    Ok(())
}

fn import_links(src: &Connection, dst: &mut Client) -> Result<()> {
    if !table_names(src)?.iter().any(|n| n == "ZLINK") { return Ok(()); }
    let (cols, rows) = read_table(src, "ZLINK")?;
    let mut tx = dst.transaction()?;
    for row in &rows {
        let source_id = int_at(&cols,row,"Z_PK");
        let book_source_id = int_at(&cols,row,"ZENTRY");
        let book_id: Option<i64> = if let Some(source_id) = book_source_id {
            tx.query_opt("SELECT id FROM books WHERE source_bookpedia_id=$1", &[&source_id])?
                .map(|r| r.get(0))
        } else { None };
        let result  = tx.execute(
            r#"INSERT INTO book_links
               (source_bookpedia_id,book_id,position,name,link_type,url,related_entry_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7)
               ON CONFLICT (source_bookpedia_id) DO UPDATE SET
                book_id=EXCLUDED.book_id, position=EXCLUDED.position, name=EXCLUDED.name,
                link_type=EXCLUDED.link_type, url=EXCLUDED.url,
                related_entry_id=EXCLUDED.related_entry_id"#,
            &[
                &source_id, &book_id, &to_i32(int_at(&cols,row,"ZPOSITION")),
                &text_at(&cols,row,"ZNAME"), &text_at(&cols,row,"ZTYPE"),
                &text_at(&cols,row,"ZURL"), &int_at(&cols,row,"Z2_ENTRY"),
            ],
        );
        print_postgres_error(result, source_id);
    }
    tx.commit()?;
    println!("Imported {} links", rows.len());
    Ok(())
}

fn import_loans(src: &Connection, dst: &mut Client) -> Result<()> {
    if !table_names(src)?.iter().any(|n| n == "ZLENTENTRY") { return Ok(()); }
    let (cols, rows) = read_table(src, "ZLENTENTRY")?;
    let mut tx = dst.transaction()?;
    for row in &rows {
        let result = tx.execute(
            r#"INSERT INTO loan_history
               (source_bookpedia_id,book_source_id,related_entry_id,borrower_name,loan_date,returned_at)
               VALUES ($1,$2,$3,$4,$5,$6)
               ON CONFLICT (source_bookpedia_id) DO UPDATE SET
                book_source_id=EXCLUDED.book_source_id, related_entry_id=EXCLUDED.related_entry_id,
                borrower_name=EXCLUDED.borrower_name, loan_date=EXCLUDED.loan_date,
                returned_at=EXCLUDED.returned_at"#,
            &[
                &int_at(&cols,row,"Z_PK"), &int_at(&cols,row,"ZENTRY"),
                &int_at(&cols,row,"Z2_ENTRY"), &text_at(&cols,row,"ZNAME"),
                &coredata_to_postgres_date(text_at(&cols,row,"ZDATE")), &coredata_to_postgres_date(text_at(&cols,row,"ZRETURNED")),
            ],
        );
        let source_id = int_at(&cols, row, "Z_PK");
        print_postgres_error(result, source_id);
    }
    tx.commit()?;
    println!("Imported {} loan-history rows", rows.len());
    Ok(())
}

fn import_recent(src: &Connection, dst: &mut Client) -> Result<()> {
    if !table_names(src)?.iter().any(|n| n == "ZLASTUSED") { return Ok(()); }
    let (cols, rows) = read_table(src, "ZLASTUSED")?;
    let mut tx = dst.transaction()?;
    for row in &rows {
        let result = tx.execute(
            r#"INSERT INTO recently_used_entries
               (source_bookpedia_id,book_source_id,related_entry_id,used_at,name)
               VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (source_bookpedia_id) DO UPDATE SET
                book_source_id=EXCLUDED.book_source_id, related_entry_id=EXCLUDED.related_entry_id,
                used_at=EXCLUDED.used_at, name=EXCLUDED.name"#,
            &[
                &int_at(&cols,row,"Z_PK"), &int_at(&cols,row,"ZENTRY"),
                &int_at(&cols,row,"Z2_ENTRY"), &coredata_to_postgres_date(text_at(&cols,row,"ZDATE")),
                &text_at(&cols,row,"ZNAME"),
            ],
        );
        let source_id = int_at(&cols, row, "Z_PK");
        print_postgres_error(result, source_id);
    }
    tx.commit()?;
    println!("Imported {} recently-used rows", rows.len());
    Ok(())
}

/// Store all source rows, including metadata tables and join tables, in an archive.
/// BLOB bytes are encoded as hex so no source information is silently discarded.
fn import_archive(src: &Connection, dst: &mut Client) -> Result<()> {
    for table in table_names(src)? {
        let (cols, rows) = read_table(src, &table)?;
        let mut tx = dst.transaction()?;
        for (ordinal, values) in rows.iter().enumerate() {
            let key = source_key(&cols, values, ordinal);
            let data: serde_json::Value = row_json(&cols, values);
            let result = tx.execute(
                r#"INSERT INTO source_archive(source_table,source_key,row_data)
                   VALUES ($1,$2,$3::jsonb)
                   ON CONFLICT (source_table,source_key) DO UPDATE SET row_data=EXCLUDED.row_data"#,
                &[&table, &key, &data],
            );
            let source_id = Some(42); //leave bogus id since no source_id
            print_postgres_error(result, source_id);
        }
        tx.commit()?;
        println!("Archived {table}: {} rows", rows.len());
    }
    Ok(())
}

/// go from sqlite .bookpd file to postgres database
pub fn migrate(src: &Connection, dst: &mut Client) -> Result<()> {
    create_schema(dst)?;
    import_books(src, dst)?;
    import_collections(src, dst)?;
    import_links(src, dst)?;
    import_loans(src, dst)?;
    import_recent(src, dst)?;
    import_archive(src, dst)?;
    Ok(())
}

/// Lightweight, incomplete verification: all source table row counts must match the archive,
/// and the normalized book count must match ZENTRY.
pub fn verify(src: &Connection, dst: &mut Client) -> Result<()> {
    println!("\n=== Source-to-archive row counts ===");
    for table in table_names(src)? {
        let source_count = row_count(src, &table)?;
        let archived_count: i64 = dst.query_one(
            "SELECT COUNT(*) FROM source_archive WHERE source_table=$1",
            &[&table],
        )?.get(0);
        let status = if source_count == archived_count { "OK" } else { "MISMATCH" };
        println!("{table}: SQLite={source_count}, archive={archived_count} [{status}]");
        if source_count != archived_count {
            bail!("archive row-count mismatch for {table}");
        }
    }

    if table_names(src)?.iter().any(|t| t == "ZENTRY") {
        let source_count = row_count(src, "ZENTRY")?;
        let target_count: i64 = dst.query_one("SELECT COUNT(*) FROM books", &[])?.get(0);
        println!("books: SQLite ZENTRY={source_count}, PostgreSQL books={target_count}");
        if source_count != target_count {
            bail!("normalized book row-count mismatch");
        }

        println!("\n=== Sample book values ===");
        let mut stmt = src.prepare(
            "SELECT Z_PK, ZTITLE, ZISBN, ZPUBLISHER FROM ZENTRY ORDER BY Z_PK LIMIT 5"
        )?;
        let samples = stmt.query_map([], |r| -> rusqlite::Result<_> {
            rusqlite::Result::Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })?;
        for sample in samples {
            let (source_id, title, isbn, publisher) = sample?;
            let target = dst.query_opt(
                "SELECT title,isbn,publisher FROM books WHERE source_bookpedia_id=$1",
                &[&source_id],
            )?;
            let target = target.context(format!("missing target book source ID {source_id}"))?;
            let actual: (Option<String>, Option<String>, Option<String>) =
                (target.get(0), target.get(1), target.get(2));
            let expected = (title, isbn, publisher);
            let matches = expected == actual;
            println!("source_id={source_id}: expected={expected:?}; actual={actual:?}; {}",
                if matches { "MATCH" } else { "MISMATCH" });
            if !matches { bail!("sample field mismatch for source ID {source_id}"); }
        }
    }
    println!("\nVerification passed.");
    Ok(())
}
