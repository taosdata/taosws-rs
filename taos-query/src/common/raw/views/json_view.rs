use std::{ffi::c_void, fmt::Debug};

use super::Offsets;
use crate::{
    common::{BorrowedValue, Ty},
    util::InlineJson,
};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct JsonView {
    // version: Version,
    pub offsets: Offsets,
    pub data: Bytes,
}

impl JsonView {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Check if the value at `row` index is NULL or not.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [methods.is_null]
    pub unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        *self.offsets.get_unchecked(row) < 0
    }

    pub unsafe fn get_unchecked(&self, row: usize) -> Option<&InlineJson> {
        let offset = self.offsets.get_unchecked(row);
        if *offset >= 0 {
            Some(InlineJson::<u16>::from_ptr(
                self.data.as_ptr().offset(*offset as isize),
            ))
        } else {
            None
        }
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        // todo: use simd_json::BorrowedValue as Json.
        self.get_unchecked(row)
            .map(|s| BorrowedValue::Json(s.as_bytes().into()))
            .unwrap_or(BorrowedValue::Null)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self.get_unchecked(row) {
            Some(json) => (Ty::Json, json.len() as _, json.as_ptr() as _),
            None => (Ty::Json, 0, std::ptr::null()),
        }
    }

    pub fn iter(&self) -> VarCharIter {
        VarCharIter { view: self, row: 0 }
    }

    pub fn to_vec(&self) -> Vec<Option<String>> {
        (0..self.len())
            .map(|row| unsafe { self.get_unchecked(row) }.map(|s| s.to_string()))
            .collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let offsets = self.offsets.as_bytes();
        wtr.write_all(offsets)?;
        wtr.write_all(&self.data)?;
        Ok(offsets.len() + self.data.len())
    }


    pub fn from_iter<S: Into<String>, T: Into<Option<S>>, I: ExactSizeIterator<Item = T>>(iter: I) -> Self {
        todo!()
    }
}

pub struct VarCharIter<'a> {
    view: &'a JsonView,
    row: usize,
}

impl<'a> Iterator for VarCharIter<'a> {
    type Item = Option<&'a InlineJson>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row <= self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.row < self.view.len() {
            let len = self.view.len() - self.row;
            (len, Some(len))
        } else {
            (0, Some(0))
        }
    }
}

impl<'a> ExactSizeIterator for VarCharIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

