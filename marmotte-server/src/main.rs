use std::time::Instant;

use figlet_rs::FIGfont;
use crate::disk_writer::DiskWriter;

mod binary;
mod binary_serializer;
mod disk_writer;

fn main() {
    let font = FIGfont::standard().unwrap();
    let figure = font.convert("Marmotte DB");
    assert!(figure.is_some());
    println!("{}", figure.unwrap());


    let mut data1 = DiskWriter::new("test1.data", 2048);
    let mut data2 = DiskWriter::new("test2.data", 2048);

    let mut iter_start = Instant::now();

    let batch_size = 1000;
    let bench_length = 30_000;

    println!("Starting to write records in batches of {}...", batch_size);

    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_size);

    for i in 1..bench_length+1 {
        let value = format!("Record number {}!", i).into_bytes();
        batch.push(value);

        if i % batch_size == 0 {
            let references: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
            data1.bulk_add_records(references);

            batch.clear();

            println!("Wrote {} records", i);
            let iter_elapsed = iter_start.elapsed();

            println!("Batch {} executed in {:?}", i, iter_elapsed);
            iter_start = Instant::now();
        }
    }
    
    println!("Starting to write records one by one...");

    for i in 1..bench_length+1 {

        data2.add_record(format!("Record number {}!", i).as_bytes());

        if i % batch_size == 0 {
            println!("Wrote {} records", i);
            let iter_elapsed = iter_start.elapsed();

            println!("Batch {} executed in {:?}", i, iter_elapsed);
            iter_start = Instant::now();
        }
    }

    println!("Done!");
}
