mod binary;
mod binary_serializer;
mod storage;
mod document;
mod indexes;

use std::time::Instant;

use figlet_rs::FIGfont;
use storage::disk_writer::DiskWriter;
use storage::disk_reader::{DiskReader, DiskReaderOptions};

fn main() {
    let font = FIGfont::standard().unwrap();
    let figure = font.convert("Marmotte DB");
    assert!(figure.is_some());
    println!("{}", figure.unwrap());

    let mut data_writer_1 = DiskWriter::new("test1.data", 2048);
    let mut data_writer_2 = DiskWriter::new("test2.data", 2048);

    let mut iter_start = Instant::now();

    let batch_size = 100;
    let bench_length = 2_000;

    println!("Starting to write records in batches of {}...", batch_size);

    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_size);

    for i in 1..bench_length+1 {
        let value = format!("Record number {}!", i).into_bytes();
        batch.push(value);

        if i % batch_size == 0 {
            let references: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
            data_writer_1.bulk_add_records(references);

            batch.clear();

            println!("Wrote {} records", i);
            let iter_elapsed = iter_start.elapsed();

            println!("Batch {} executed in {:?}", i, iter_elapsed);
            iter_start = Instant::now();
        }
    }
    
    println!("Starting to write records one by one...");

    for i in 1..bench_length+1 {

        data_writer_2.add_record(format!("Record number {}!", i).as_bytes());

        if i % batch_size == 0 {
            println!("Wrote {} records", i);
            let iter_elapsed = iter_start.elapsed();

            println!("Batch {} executed in {:?}", i, iter_elapsed);
            iter_start = Instant::now();
        }
    }

    println!("Write bench done!");

    println!("Starting to read records...");

    let mut data_reader_1 = DiskReader::new("test1.data", DiskReaderOptions::create_default());
    let mut data_reader_2 = DiskReader::new("test2.data", DiskReaderOptions::create_default());

    println!("Reading records of test1.data ...");

    for item in data_reader_1 {
        let text = String::from_utf8(item.content).unwrap();
        println!("{}", text);
    }

    println!("Reading records of test2.data ...");
    for item in data_reader_2 {
        let text = String::from_utf8(item.content).unwrap();
        println!("{}", text);
    }

}
