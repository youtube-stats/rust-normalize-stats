extern crate chrono;
extern crate postgres;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const QUERY: &'static str = "SELECT time FROM youtube.stats.metrics ORDER BY time ASC limit 1";

use postgres::{TlsMode,Connection,rows};
use chrono::Local;
use postgres::rows::Row;
use std::time::SystemTime;

fn main() {
    println!("Hello, world!");

    let params: &'static str = POSTGRESQL_URL;
    let tls: TlsMode = TlsMode::None;
    let conn: Connection = Connection::connect(params, tls).unwrap();

    let mut count: u32 = 0;

    loop {
        println!("Grabbing row to insert");
        println!("Inserted {} rows", count);

        let query: &'static str = QUERY;
        let rows: rows::Rows = conn.query(query, &[]).unwrap();
        let row: Row = rows.iter().last().unwrap();

        let time: chrono::DateTime<Local> = row.get(0);
        let id: String = row.get(1);
        let subs: String = row.get(2);
        let views: String = row.get(3);
        let videos: String = row.get(4);

        println!("Retrieved (time={}, id={}, subs={}, views={}, videos={})",
        time, id, subs, views, videos);
    }
}
