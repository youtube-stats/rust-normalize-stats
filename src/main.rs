extern crate chrono;
extern crate postgres;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const QUERY: &'static str = "SELECT time, channel_id, subs::int, views::bigint, videos::int FROM youtube.stats.metrics ORDER BY time ASC limit 1";

use postgres::{TlsMode,Connection,rows};
use chrono::Local;
use postgres::rows::{Row,Rows};

struct Metric {
    time: chrono::DateTime<Local>,
    id: u32,
    subs: u32,
    views: i64,
    videos: u32
}

fn main() {
    println!("Hello, world!");

    let params: &'static str = POSTGRESQL_URL;
    let tls: TlsMode = TlsMode::None;
    let conn: Connection = Connection::connect(params, tls).unwrap();

    let mut count: u32 = 0;

    loop {
        println!("Inserted {} rows", count);
        count += 1;

        let query: &'static str = QUERY;
        let rows: Rows = conn.query(query, &[]).unwrap();
        let row: Row = rows.iter().last().unwrap();

        let time: chrono::DateTime<Local> = row.get(0);
        let id: i32 = row.get(1);
        let subs: i32 = row.get(2);
        let views: i64 = row.get(3);
        let videos: i32 = row.get(4);

        println!("Retrieved (time={}, id={}, subs={}, views={}, videos={})",
        time, id, subs, views, videos);
    }
}
