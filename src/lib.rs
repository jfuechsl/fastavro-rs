pub use c_str_macro::c_str;

mod reader;
mod value;

pub use reader::{Reader, AvroError};
pub use value::Value;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use chrono::Utc;

    #[test]
    fn read_records() {
        let filepath = Path::new("tmp/data/testrecords.avro");
        let reader = reader::Reader::from_file(filepath).expect("Ooops");
        let mut record_count = 0;
        let start = Utc::now();
        for rec in reader {
            record_count += 1;
            let bids = rec.get_by_name(c_str!("bids")).unwrap();
            let size = bids.get_size().unwrap();
            for i in 0..size {
                let _bid = bids.get_by_index(i).unwrap();
            }
            let asks = rec.get_by_name(c_str!("asks")).unwrap();
            let size = asks.get_size().unwrap();
            for i in 0..size {
                let _ask = asks.get_by_index(i).unwrap();
            }
        }
        assert!(record_count > 17_000);
        println!("Records: {}", record_count);
        println!("Took: {}", Utc::now()-start);
    }
}
