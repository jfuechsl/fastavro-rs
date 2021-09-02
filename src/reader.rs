use std::path::Path;
use std::ffi::{CString, CStr};
use libavro_sys::{avro_file_reader_t, avro_file_reader, avro_strerror, avro_generic_class_from_schema, avro_file_reader_get_writer_schema, avro_value_iface_t, avro_value_t, avro_generic_value_new, avro_file_reader_read_value, avro_file_reader_close, avro_schema_decref, avro_schema_t};
use std::ptr::null_mut;
use thiserror::Error;
use crate::value::Value;

// TODO: optimization: reuse one record for the reader

#[derive(Error, Debug, Clone)]
pub enum AvroError {
    #[error("Avro read error: {0}")]
    ReaderError(String)
}

pub struct Reader {
    reader: avro_file_reader_t,
    writer_schema: avro_schema_t,
    record_class: *mut avro_value_iface_t,
    // current_record: avro_value_t,
    is_eof: bool,
}

impl Reader {
    pub fn from_file(file: &Path) -> anyhow::Result<Self> {
        let filename = CString::new(file.to_str().unwrap())?;
        let mut reader: avro_file_reader_t = null_mut();
        #[allow(unused_assignments)]
        let mut record_class: *mut avro_value_iface_t = null_mut();
        #[allow(unused_assignments)]
        let mut writer_schema: avro_schema_t = null_mut();
        // let mut current_record: avro_value_t = avro_value_t { iface: null_mut(), self_: null_mut() };
        unsafe {
            if avro_file_reader(filename.as_ptr(), &mut reader) != 0 {
                let errstr = CStr::from_ptr(avro_strerror());
                return Err(AvroError::ReaderError(String::from(errstr.to_str().unwrap())).into())
            }
            writer_schema = avro_file_reader_get_writer_schema(reader);
            record_class = avro_generic_class_from_schema(writer_schema);
            // avro_generic_value_new(record_class, &mut current_record);
        }
        Ok(Self {
            reader,
            #[allow(dead_code)]
            writer_schema,
            #[allow(dead_code)]
            record_class,
            // current_record,
            is_eof: false,
        })
    }

    fn new_empty_record(&self) -> avro_value_t {
        let mut record: avro_value_t = avro_value_t { iface: null_mut(), self_: null_mut() };
        unsafe {
            avro_generic_value_new(self.record_class, &mut record);
        }
        record
    }
}

impl Iterator for Reader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_eof {
            return None;
        }
        let mut record = self.new_empty_record();
        unsafe {
            let rval = avro_file_reader_read_value(self.reader, &mut record);
            if rval == -1 { // EOF
                self.is_eof = true;
                return None;
            } else if rval != 0 {
                let errstr = CStr::from_ptr(avro_strerror());
                panic!("{}", errstr.to_str().unwrap());
            }
        }
        Some(Value::from_avro_record(record))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        unsafe {
            avro_file_reader_close(self.reader);
            (*self.record_class).decref_iface.unwrap()(self.record_class);
            avro_schema_decref(self.writer_schema);
            avro_schema_decref(self.writer_schema); // IDK why
            self.record_class = null_mut();
        }
        self.is_eof = false;
    }
}