use std::{ffi::c_void, fmt::Debug};

use super::Offsets;
use crate::{
    common::{BorrowedValue, Ty},
    prelude::InlinableWrite,
    util::InlineStr,
};

use bytes::Bytes;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct VarCharView {
    // version: Version,
    pub(crate) offsets: Offsets,
    pub(crate) data: Bytes,
}

impl VarCharView {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> VarCharNullsIter {
        VarCharNullsIter {
            view: &self,
            row: 0,
        }
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
    }

    /// Check if the value at `row` index is NULL or not.
    ///
    /// Returns null when `row` index out of bound.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [is_null](#method.is_null)
    pub(crate) unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        *self.offsets.get_unchecked(row) < 0
    }

    pub(crate) unsafe fn get_unchecked(&self, row: usize) -> Option<&InlineStr> {
        let offset = self.offsets.get_unchecked(row);
        if *offset >= 0 {
            Some(InlineStr::<u16>::from_ptr(
                self.data.as_ptr().offset(*offset as isize),
            ))
        } else {
            None
        }
    }

    pub(crate) unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map(|s| BorrowedValue::VarChar(s.as_str()))
            .unwrap_or(BorrowedValue::Null)
    }

    pub(crate) unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self.get_unchecked(row) {
            Some(s) => (Ty::VarChar, s.len() as _, s.as_ptr() as _),
            None => (Ty::VarChar, 0, std::ptr::null()),
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
    pub fn iter_as_bytes(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.len()).map(|row| unsafe { self.get_unchecked(row) }.map(|s| s.as_bytes()))
    }

    pub fn to_bytes_vec(&self) -> Vec<Option<&[u8]>> {
        self.iter_as_bytes().collect_vec()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let offsets = self.offsets.as_bytes();
        wtr.write_all(offsets)?;
        wtr.write_all(&self.data)?;
        Ok(offsets.len() + self.data.len())
    }

    pub fn from_iter<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        let mut offsets = Vec::new();
        let mut data = Vec::new();

        for i in iter.into_iter().map(|v| v.into()) {
            if let Some(s) = i {
                let s: &str = s.as_ref();
                offsets.push(data.len() as i32);
                data.write_inlined_str::<2>(&s).unwrap();
            } else {
                offsets.push(-1);
            }
        }
        let offsets_bytes = unsafe {
            Vec::from_raw_parts(
                offsets.as_mut_ptr() as *mut u8,
                offsets.len() * 4,
                offsets.capacity() * 4,
            )
        };
        std::mem::forget(offsets);
        VarCharView {
            offsets: Offsets(offsets_bytes.into()),
            data: data.into(),
        }
    }
}

pub struct VarCharIter<'a> {
    view: &'a VarCharView,
    row: usize,
}

impl<'a> Iterator for VarCharIter<'a> {
    type Item = Option<&'a InlineStr>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row <= self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for VarCharIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

pub struct VarCharNullsIter<'a> {
    view: &'a VarCharView,
    row: usize,
}

impl<'a> Iterator for VarCharNullsIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row <= self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.is_null_unchecked(row) })
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for VarCharNullsIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}
