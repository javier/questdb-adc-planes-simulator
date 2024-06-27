use chrono::Utc;
use rand::Rng;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::sync::Semaphore;
use tokio::time::{Duration, interval};
use questdb::ingress::{Sender, Buffer, TimestampNanos};
use structopt::StructOpt;
use futures::future::join_all;

#[derive(StructOpt, Debug)]
#[structopt(name = "flight-data-generator")]
struct Opt {
    #[structopt(long)]
    connection_string: String,

    #[structopt(long)]
    total_rows: u64,

    #[structopt(long)]
    rate_per_plane: u64,

    #[structopt(long)]
    plane_count: u32,

    #[structopt(long)]
    table_name: String,
}

#[derive(Clone)]
struct PlaneData {
    plane_id: String,
    timestamp: i64,
    airspeed: f64,
    altitude: f64,
    pitch: f64,
    roll: f64,
    yaw: f64,
    aoa: f64,
    oat: f64,
}

impl PlaneData {
    fn new(plane_id: String) -> Self {
        let mut rng = rand::thread_rng();
        PlaneData {
            plane_id,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap(),
            airspeed: rng.gen_range(200.0..300.0),
            altitude: rng.gen_range(30000.0..40000.0),
            pitch: rng.gen_range(-10.0..10.0),
            roll: rng.gen_range(-10.0..10.0),
            yaw: rng.gen_range(-10.0..10.0),
            aoa: rng.gen_range(0.0..15.0),
            oat: rng.gen_range(-60.0..20.0),
        }
    }

    fn update(&mut self) {
        let mut rng = rand::thread_rng();
        self.timestamp = Utc::now().timestamp_nanos_opt().unwrap();
        self.airspeed = (self.airspeed + rng.gen_range(-1.0..1.0)).clamp(200.0, 300.0);
        self.altitude = (self.altitude + rng.gen_range(-10.0..10.0)).clamp(30000.0, 40000.0);
        self.pitch = (self.pitch + rng.gen_range(-1.0..1.0)).clamp(-10.0, 10.0);
        self.roll = (self.roll + rng.gen_range(-1.0..1.0)).clamp(-10.0, 10.0);
        self.yaw = (self.yaw + rng.gen_range(-1.0..1.0)).clamp(-10.0, 10.0);
        self.aoa = (self.aoa + rng.gen_range(-0.5..0.5)).clamp(0.0, 15.0);
        self.oat = (self.oat + rng.gen_range(-1.0..1.0)).clamp(-60.0, 20.0);
    }
}

fn generate_plane_id(n: u32) -> String {
    let letters = ((n / 1000) as u8, ((n / 100) % 10) as u8);
    let digits = n % 100;
    format!(
        "{}{}{:02}",
        (letters.0 + b'A') as char,
        (letters.1 + b'A') as char,
        digits
    )
}

async fn generate_data(
    sender: Arc<tokio::sync::Mutex<Sender>>,
    plane_id: String,
    rate: u64,
    total_rows: Arc<AtomicU64>,
    sem: Arc<Semaphore>,
    table_name: Arc<String>,
) {
    let mut plane_data = PlaneData::new(plane_id);
    let mut interval = interval(Duration::from_millis(1000 / rate));
    let mut rows_generated = 0;

    while total_rows.load(Ordering::SeqCst) > 0 {
        interval.tick().await;

        if total_rows.fetch_sub(1, Ordering::SeqCst) == 0 {
            break;
        }

        plane_data.update();
        rows_generated += 1;

        let _permit = sem.acquire().await.unwrap();
        let mut buffer = Buffer::new();
        buffer.table(&table_name as &str).unwrap()
            .symbol("plane_id", &plane_data.plane_id).unwrap()
            .column_f64("airspeed", plane_data.airspeed).unwrap()
            .column_f64("altitude", plane_data.altitude).unwrap()
            .column_f64("pitch", plane_data.pitch).unwrap()
            .column_f64("roll", plane_data.roll).unwrap()
            .column_f64("yaw", plane_data.yaw).unwrap()
            .column_f64("aoa", plane_data.aoa).unwrap()
            .column_f64("oat", plane_data.oat).unwrap()
            .at(TimestampNanos::new(plane_data.timestamp)).unwrap();

        let mut sender = sender.lock().await;
        match sender.flush(&mut buffer) {
            Ok(_) => println!("Successfully flushed buffer for plane {} with {} rows", plane_data.plane_id, rows_generated),
            Err(e) => eprintln!("Failed to flush buffer for plane {}: {}", plane_data.plane_id, e),
        }

        if rows_generated % 1000 == 0 {
            println!("Plane {} generated {} rows so far. Total remaining rows: {}", plane_data.plane_id, rows_generated, total_rows.load(Ordering::SeqCst));
        }
    }

    println!("Plane {} generated {} rows.", plane_data.plane_id, rows_generated);
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let sender = Arc::new(
        tokio::sync::Mutex::new(Sender::from_conf(&opt.connection_string).expect("Failed to connect to QuestDB")),
    );
    let total_rows = Arc::new(AtomicU64::new(opt.total_rows));
    let rate_per_plane = opt.rate_per_plane;
    let plane_count = opt.plane_count;
    let sem = Arc::new(Semaphore::new(plane_count as usize * rate_per_plane as usize));
    let table_name = Arc::new(opt.table_name.clone());

    let mut tasks = vec![];

    for plane_id in 0..plane_count {
        let sender = sender.clone();
        let total_rows = total_rows.clone();
        let sem = sem.clone();
        let table_name = table_name.clone();
        let plane_id_str = generate_plane_id(plane_id);
        tasks.push(tokio::spawn(generate_data(sender, plane_id_str, rate_per_plane, total_rows, sem, table_name)));
    }

    join_all(tasks).await;

    let generated_rows = opt.total_rows - total_rows.load(Ordering::SeqCst);
    println!("Data generation completed. Total rows generated: {}", generated_rows);
}
