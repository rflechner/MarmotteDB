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


    let mut w = DiskWriter::new("test3.data", 2048);

    for i in 0..10_000 {
        w.add_record(format!("Record number {}!", i).as_bytes());

        if i % 1000 == 0 {
            println!("Wrote {} records", i);
        }
    }

    println!("Done!");
}
