use libavro_sys::{avro_value_t, size_t};
use std::ptr::null_mut;
use std::ffi::CStr;

#[derive(Debug)]
pub struct Value {
    inner: avro_value_t,
    is_owner: bool,
}

impl Value {
    pub fn from_avro_record(record: avro_value_t) -> Self {
        Self {
            inner: record,
            is_owner: true,
        }
    }

    pub fn from_array_elem(elem: avro_value_t) -> Self {
        Self {
            inner: elem,
            is_owner: false,
        }
    }

    pub fn from_record_elem(elem: avro_value_t) -> Self {
        Self {
            inner: elem,
            is_owner: false,
        }
    }

    pub fn get_by_name(&self, name: &CStr) -> Option<Value> {
        let mut value: avro_value_t = avro_value_t { self_: null_mut(), iface: null_mut() };
        let mut index: u64 = 0;
        unsafe {
            let rval = (*self.inner.iface).get_by_name.unwrap()(
                self.inner.iface, self.inner.self_,
                name.as_ptr(), &mut value, &mut index
            );
            if rval != 0 {
                // let errstr = CStr::from_ptr(avro_strerror());
                // println!("get_by_name: {}", errstr.to_str().unwrap());
                None
            } else {
                Some(Value::from_record_elem(value))
            }
        }
    }

    pub fn get_double(&self) -> Option<f64> {
        unsafe {
            let mut val = 0.0;
            let rval = (*self.inner.iface).get_double.unwrap()(
                self.inner.iface, self.inner.self_, &mut val
            );
            if rval != 0 {
                None
            } else {
                Some(val)
            }
        }
    }

    pub fn get_int(&self) -> Option<i32> {
        unsafe {
            let mut val: i32 = 0;
            let rval = (*self.inner.iface).get_int.unwrap()(
                self.inner.iface, self.inner.self_, &mut val
            );
            if rval != 0 {
                None
            } else {
                Some(val)
            }
        }
    }

    pub fn get_size(&self) -> Option<usize> {
        unsafe {
            let mut size: size_t = 0;
            let rval = (*self.inner.iface).get_size.unwrap()(
                self.inner.iface, self.inner.self_, &mut size,
            );
            if rval != 0 {
                // let errstr = CStr::from_ptr(avro_strerror());
                // println!("get_by_name: {}", errstr.to_str().unwrap());
                None
            } else {
                Some(size as usize)
            }
        }
    }

    pub fn get_by_index(&self, index: usize) -> Option<Value> {
        unsafe {
            let mut value: avro_value_t = avro_value_t { self_: null_mut(), iface: null_mut() };
            let rval = (*self.inner.iface).get_by_index.unwrap()(
                self.inner.iface, self.inner.self_, index as size_t,
                &mut value, null_mut() // IDK if this is alright
            );
            if rval != 0 {
                // let errstr = CStr::from_ptr(avro_strerror());
                // println!("get_by_name: {}", errstr.to_str().unwrap());
                None
            } else {
                Some(Value::from_array_elem(value))
            }
        }
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        unsafe {
            if self.is_owner {
                (*self.inner.iface).decref.unwrap()(&mut self.inner);
                (*self.inner.iface).decref_iface.unwrap()(self.inner.iface);
            }
        }
    }
}