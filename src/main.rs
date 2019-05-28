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
    views: u64,
    videos: u32
}

fn get_row(conn: &Connection) -> Option<Metric> {
    let query: &'static str = QUERY;
    let rows: Rows = conn.query(query, &[]).unwrap();
    let row_option: Option<Row> = rows.iter().last();
    if row_option.is_none() {
        return None;
    }

    let row: Row = row_option.unwrap();
    let time: chrono::DateTime<Local> = row.get(0);

    let id: i32 = row.get(1);
    let id: u32 = id as u32;

    let subs: i32 = row.get(2);
    let subs: u32 = subs as u32;

    let views: i64 = row.get(3);
    let views: u64 = views as u64;

    let videos: i32 = row.get(4);
    let videos: u32 = videos as u32;

    Some(Metric {
        time,
        id,
        subs,
        views,
        videos
    })
}

impl Metric {
    pub fn string(&self) -> String {
        format!("Metric({},{},{},{},{})", self.time, self.id, self.subs, self.views, self.videos)
    }
}

fn main() {
    println!("init!");

    let params: &'static str = POSTGRESQL_URL;
    let tls: TlsMode = TlsMode::None;
    let conn: Connection = Connection::connect(params, tls).unwrap();

    let mut count: u32 = 0;

    loop {
        println!("Inserted {} rows", count);
        count += 1;

        let row_option: Option<Metric> = get_row(&conn);
        if row_option.is_none() {
            eprintln!("No rows");
            break;
        }

        let row: Metrics = row_option.unwrap();
        println!("Retrieved row {}", row.string());


    }

    println!("done");
}
