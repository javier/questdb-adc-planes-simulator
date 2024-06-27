use chrono::Utc;
use rand::Rng;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::sync::Semaphore;
use tokio::time::{Duration, interval};
use questdb::{
    Result,
    ingress::{Sender, Buffer, TimestampNanos}
};
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

    #[structopt(long, default_value = "AA00")]
    starting_plane_id: String,

    #[structopt(long)]
    quiet: bool,

    #[structopt(long, default_value = "1000")]
    batch_size: usize,
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

fn generate_plane_id(starting_id: &str, n: u32) -> String {
    let letters = &starting_id[..2];
    let digits = starting_id[2..].parse::<u32>().unwrap() + n;
    format!("{}{:02}", letters, digits)
}

async fn generate_data(
    sender: Arc<tokio::sync::Mutex<Sender>>,
    plane_id: String,
    rate: u64,
    total_rows: Arc<AtomicU64>,
    sem: Arc<Semaphore>,
    table_name: Arc<String>,
    quiet: bool,
    batch_size: usize, // Batch size per plane
) {
    let mut plane_data = PlaneData::new(plane_id);
    let mut interval = interval(Duration::from_millis(1000 / rate));
    let mut rows_generated = 0;
    let mut buffer = Buffer::new();

    loop {
        interval.tick().await;

        if total_rows.load(Ordering::SeqCst) == 0 {
            break;
        }

        plane_data.update();
        rows_generated += 1;

        buffer.table(table_name.as_str()).unwrap()
            .symbol("plane_id", &plane_data.plane_id).unwrap()
            .column_f64("airspeed", plane_data.airspeed).unwrap()
            .column_f64("altitude", plane_data.altitude).unwrap()
            .column_f64("pitch", plane_data.pitch).unwrap()
            .column_f64("roll", plane_data.roll).unwrap()
            .column_f64("yaw", plane_data.yaw).unwrap()
            .column_f64("aoa", plane_data.aoa).unwrap()
            .column_f64("oat", plane_data.oat).unwrap()
            .at(TimestampNanos::new(plane_data.timestamp)).unwrap();

        // Decrement the total_rows only when we have successfully added to the buffer
        let remaining_rows = total_rows.fetch_sub(1, Ordering::SeqCst);
        if remaining_rows == 0 {
            break;
        }

        // Flush buffer when batch size is reached
        if rows_generated % batch_size == 0 {
            let _permit = sem.acquire().await.unwrap();
            let mut sender = sender.lock().await;
            if !quiet {
                match sender.flush(&mut buffer) {
                    Ok(_) => println!("Successfully flushed buffer for plane {} with {} rows", plane_data.plane_id, rows_generated),
                    Err(e) => eprintln!("Failed to flush buffer for plane {}: {}", plane_data.plane_id, e),
                }
            } else {
                let _ = sender.flush(&mut buffer);
            }
        }
    }

    // Flush any remaining rows in the buffer
    if buffer.len() > 0 {
        let _permit = sem.acquire().await.unwrap();
        let mut sender = sender.lock().await;
        let _ = sender.flush(&mut buffer);
    }

    if !quiet {
        println!("Plane {} generated {} rows.", plane_data.plane_id, rows_generated);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let connection_string = opt.connection_string.clone();
    let sender = Arc::new(
        tokio::sync::Mutex::new(Sender::from_conf(&connection_string)?),
    );
    let total_rows = Arc::new(AtomicU64::new(opt.total_rows));
    let rate_per_plane = opt.rate_per_plane;
    let plane_count = opt.plane_count;
    let sem = Arc::new(Semaphore::new(plane_count as usize * opt.batch_size));
    let table_name = Arc::new(opt.table_name.clone());
    let starting_plane_id = opt.starting_plane_id.clone();
    let quiet = opt.quiet;
    let batch_size = opt.batch_size;

    let mut tasks = vec![];

    for plane_id in 0..plane_count {
        let sender = sender.clone();
        let total_rows = total_rows.clone();
        let sem = sem.clone();
        let table_name = table_name.clone();
        let plane_id_str = generate_plane_id(&starting_plane_id, plane_id);
        tasks.push(tokio::spawn(generate_data(sender, plane_id_str, rate_per_plane, total_rows, sem, table_name, quiet, batch_size)));
    }

    join_all(tasks).await;

    let generated_rows = opt.total_rows - total_rows.load(Ordering::SeqCst);
    println!("Data generation completed. Total rows generated: {}", generated_rows);

    Ok(())
}
