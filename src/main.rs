extern crate chrono;
extern crate postgres;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const QUERY: &'static str = "SELECT time, channel_id, subs::int, views::bigint, videos::int FROM youtube.stats.metrics ORDER BY time ASC limit 1";
const DELETE: &'static str = "DELETE FROM youtube.stats.metrics WHERE time = $1 AND channel_id = $2 AND subs = $3 AND views = $4 AND videos = $5";

use postgres::{TlsMode,Connection};
use chrono::Local;
use postgres::rows::{Row,Rows};

struct Metric {
    time: chrono::DateTime<Local>,
    id: i32,
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

fn delete_row(conn: &Connection, row: &Metric) {
    println!("Deleting row {}", row.string());
    let query: &'static str = DELETE;

    conn.execute(query, &[row.time, row.id, row.subs, row.views, row.videos]).unwrap();
}

impl Metric {
    pub fn string(&self) -> String {
        format!("Metric({},{},{},{},{})", self.time, self.id, self.subs, self.views, self.videos)
    }
}

mod check {
    use postgres::Connection;
    use postgres::rows::{Row,Rows};

    const SUBS_SELECT: &'static str = "SELECT subs::int FROM youtube.stats.metric_subs WHERE channel_id = $1 ORDER BY time ASC LIMIT 1";
    const VIEWS_SELECT: &'static str = "SELECT views::bigint FROM youtube.stats.metric_views WHERE channel_id = $1 ORDER BY time ASC LIMIT 1";
    const VIDEOS_SELECT: &'static str = "SELECT videos::int FROM youtube.stats.metric_videos WHERE channel_id = $1 ORDER BY time ASC LIMIT 1";

    pub fn subs(conn: &Connection, id: &i32, subs_incumbent: &u32) -> bool {
        let query: &'static str = SUBS_SELECT;

        let rows: Rows = conn.query(query, &[id]).unwrap();
        let row_option: Option<Row> = rows.iter().last();
        if row_option.is_none() {
            return true;
        }

        let row: Row = row_option.unwrap();
        let subs: i32 = row.get(0);
        let subs: u32 = subs as u32;

        subs != *subs_incumbent
    }

    pub fn views(conn: &Connection, id: &i32, views_incumbent: &u64) -> bool {
        let query: &'static str = VIEWS_SELECT;

        let rows: Rows = conn.query(query, &[id]).unwrap();
        let row_option: Option<Row> = rows.iter().last();
        if row_option.is_none() {
            return true;
        }

        let row: Row = row_option.unwrap();
        let views: i64 = row.get(0);
        let views: u64 = views as u64;

        views != *views_incumbent
    }

pub fn videos(conn: &Connection, id: &i32, videos_incumbent: &u32) -> bool {
        let query: &'static str = VIDEOS_SELECT;

        let rows: Rows = conn.query(query, &[id]).unwrap();
        let row_option: Option<Row> = rows.iter().last();
        if row_option.is_none() {
            return true;
        }

        let row: Row = row_option.unwrap();
        let videos: i32 = row.get(0);
        let videos: u32 = videos as u32;

        videos != *videos_incumbent
    }
}

mod insert {
    use postgres::Connection;
    use postgres::rows::{Row,Rows};
    use chrono::Local;

    const SUBS_INSERT: &'static str = "INSERT INTO youtube.stats.metric_subs (time, channel_id, subs) VALUES ($1, $2, $3);";
    const VIEWS_INSERT: &'static str = "INSERT INTO youtube.stats.metric_views (time, channel_id, views) VALUES ($1, $2, $3);";
    const VIDEOS_INSERT: &'static str = "INSERT INTO youtube.stats.metric_videos (time, channel_id, videos) VALUES ($1, $2, $3);";

    pub fn subs_insert(conn: &Connection, time: &chrono::DateTime<Local>, id: &i32, subs: &u32) {
        println!("Inserting {} into subs table", row.string());
        let query: &'static str = SUBS_INSERT;

        let output: u64 = conn.execute(query, &[time, id, subs]).unwrap();
        if output != 1 {
            eprintln!("Could not insert row {} {} {}", time, id, subs);
        }
    }

    pub fn views_insert(conn: &Connection, time: &chrono::DateTime<Local>, id: &i32, views: &u64) {
        println!("Inserting {} into views table", row.string());
        let query: &'static str = VIEWS_INSERT;

        let output: u64 = conn.execute(query, &[time, id, views]).unwrap();
        if output != 1 {
            eprintln!("Could not insert row {} {} {}", time, id, views);
        }
    }

    pub fn videos_insert(conn: &Connection, time: &chrono::DateTime<Local>, id: &i32, videos: &u32) {
        println!("Inserting {} into videos table", row.string());
        let query: &'static str = VIDEOS_INSERT;

        let output: u64 = conn.execute(query, &[time, id, videos]).unwrap();
        if output != 1 {
            eprintln!("Could not insert row {} {} {}", time, id, videos);
        }
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

        let row: Metric = row_option.unwrap();
        println!("Retrieved row {}", row.string());

        let subs_insert_pred: bool = check::subs(&conn, &row.id, &row.subs);
        let views_insert_pred: bool = check::views(&conn, &row.id, &row.views);
        let videos_insert_pred: bool = check::videos(&conn, &row.id, &row.videos);

        println!("Subs {}", subs_insert_pred);
        println!("Views {}", views_insert_pred);
        println!("Videos {}", videos_insert_pred);

        if subs_insert_pred {
            insert::subs_insert(&conn, &row.time, &row.id, &row.subs)
        }

        if views_insert_pred {
            insert::views_insert(&conn, &row.time, &row.id, &row.views)
        }

        if videos_insert_pred {
            insert::videos_insert(&conn, &row.time, &row.id, &row.videos)
        }

        delete_row(&conn, &row);
    }

    println!("done");
}
