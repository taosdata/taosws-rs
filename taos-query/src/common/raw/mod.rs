use crate::{
    common::{BorrowedValue, Column, Field, Precision, Timestamp, Ty, Value},
    util::{Inlinable, InlinableRead, InlinableWrite, InlineStr},
};
use bitvec::macros::internal::funty::Numeric;
use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;
use once_cell::unsync::OnceCell;
use serde::Deserialize;

use std::{
    borrow::Cow, collections::HashMap, ffi::c_void, hash::Hash, ops::Deref, ptr::NonNull, slice,
};
use std::{fmt::Debug, mem::size_of, mem::transmute};

pub mod layout;
pub mod meta;

use layout::Layout;
// #[derive(Debug, Clone, Copy)]
// #[repr(C)]
// pub enum Layout {
//     V2Ptr,
//     V2Raw,
//     Ref,
//     Owned,
// }

pub mod views;

pub use views::ColumnView;
use views::*;

pub use meta::*;

mod de;
mod rows;
pub use rows::*;

/// Raw data block format (B for bytes):
///
/// ```text,ignore
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// | len | group id | col_schema... | length... | (bitmap or offsets    | col data)   ... |
/// | 4B  | 8B       | (2+4)B * cols | 4B * cols | (row+7)/8 or 4 * rows | length[col] ... |
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// ```
///
/// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
/// recorded in the first segment, next to the struct header
// #[derive(Debug)]
pub struct RawData {
    /// Layout is auto detected.
    layout: Layout,
    /// Data is required, which could be v2 websocket block or a v3 raw block.
    data: Bytes,
    /// Number of rows in current data block.
    rows: usize,
    /// Number of columns (or fields) in current data block.
    cols: usize,
    /// Timestamp precision in current data block.
    precision: Precision,
    /// Database name, if is tmq message.
    database: Option<String>,
    /// Table name of current data block.
    table: Option<String>,
    /// Field names of current data block.
    fields: Vec<String>,
    // todo: is raw fields necessary?
    raw_fields: Vec<Field>,
    /// Group id in current data block, it always be 0 in v2 block, and be meaningful in v3.
    group_id: u64,
    /// Column schemas of current data block, contains only data type and the length defined in `create table`.
    schemas: Schemas,
    /// Data lengths collection for all columns.
    lengths: Lengths,
    /// A vector of [ColumnView] that represent column of values efficiently.
    columns: Vec<ColumnView>,
}

impl Debug for RawData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // todo: more helpful debug impl.
        f.debug_struct("Raw")
            .field("layout", &self.layout)
            .field("data", &"...")
            .field("rows", &self.rows)
            .field("cols", &self.cols)
            .field("precision", &self.precision)
            .field("table", &self.table)
            .field("fields", &self.fields)
            .field("raw_fields", &self.raw_fields)
            .field("group_id", &self.group_id)
            .field("schemas", &self.schemas)
            .field("lengths", &self.lengths)
            .field("columns", &self.columns)
            .finish()
    }
}

impl RawData {
    pub unsafe fn parse_from_ptr(
        ptr: *mut c_void,
        rows: usize,
        cols: usize,
        precision: Precision,
    ) -> Self {
        let len = *(ptr as *const u32) as usize;
        let bytes = std::slice::from_raw_parts(ptr as *const u8, len);
        let bytes = Bytes::from(bytes);
        Self::parse_from_raw_block(bytes, rows, cols, precision).with_layout(Layout::default())
    }

    pub fn parse_from_ptr_v2(
        ptr: *const *const c_void,
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        todo!()
    }

    pub fn parse_from_raw_block_v2(
        bytes: impl Into<Bytes>,
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        use bytes::BufMut;
        debug_assert_eq!(fields.len(), lengths.len());

        const BOOL_NULL: u64 = 0x02;
        const TINY_INT_NULL: u64 = 0x80;
        const SMALL_INT_NULL: u64 = 0x8000;
        const INT_NULL: u64 = 0x80000000;
        const BIG_INT_NULL: u64 = 0x8000000000000000;
        const FLOAT_NULL: u64 = 0x7FF00000;
        const DOUBLE_NULL: u64 = 0x7FFFFF0000000000;
        const U_TINY_INT_NULL: u64 = 0xFF;
        const U_SMALL_INT_NULL: u64 = 0xFFFF;
        const U_INT_NULL: u64 = 0xFFFFFFFF;
        const U_BIG_INT_NULL: u64 = 0xFFFFFFFFFFFFFFFF;

        const fn bool_is_null(v: *const bool) -> bool {
            unsafe { *(v as *const u8) == 0x02 }
        }
        const fn tiny_int_is_null(v: *const i8) -> bool {
            unsafe { *(v as *const u8) == 0x80 }
        }
        const fn small_int_is_null(v: *const i16) -> bool {
            unsafe { *(v as *const u16) == 0x8000 }
        }
        const fn int_is_null(v: *const i32) -> bool {
            unsafe { *(v as *const u32) == 0x80000000 }
        }
        const fn big_int_is_null(v: *const i64) -> bool {
            unsafe { *(v as *const u64) == 0x8000000000000000 }
        }
        const fn u_tiny_int_is_null(v: *const u8) -> bool {
            unsafe { *(v as *const u8) == 0xFF }
        }
        const fn u_small_int_is_null(v: *const u16) -> bool {
            unsafe { *(v as *const u16) == 0xFFFF }
        }
        const fn u_int_is_null(v: *const u32) -> bool {
            unsafe { *(v as *const u32) == 0xFFFFFFFF }
        }
        const fn u_big_int_is_null(v: *const u64) -> bool {
            unsafe { *(v as *const u64) == 0xFFFFFFFFFFFFFFFF }
        }
        const fn float_is_null(v: *const f32) -> bool {
            unsafe { *(v as *const u32) == 0x7FF00000 }
        }
        const fn double_is_null(v: *const f64) -> bool {
            unsafe { *(v as *const u64) == 0x7FFFFF0000000000 }
        }

        // const BOOL_NULL: u8 = 0x2;
        // const TINY_INT_NULL: i8 = i8::MIN;
        // const SMALL_INT_NULL: i16 = i16::MIN;
        // const INT_NULL: i32 = i32::MIN;
        // const BIG_INT_NULL: i64 = i64::MIN;
        // const FLOAT_NULL: f32 = 0x7FF00000i32 as f32;
        // const DOUBLE_NULL: f64 = 0x7FFFFF0000000000i64 as f64;
        // const U_TINY_INT_NULL: u8 = u8::MAX;
        // const U_SMALL_INT_NULL: u16 = u16::MAX;
        // const U_INT_NULL: u32 = u32::MAX;
        // const U_BIG_INT_NULL: u64 = u64::MAX;

        let bytes = bytes.into();
        let cols = fields.len();
        let mut schemas_bytes =
            bytes::BytesMut::with_capacity(rows * std::mem::size_of::<ColSchema>());
        fields
            .iter()
            .for_each(|f| schemas_bytes.put(f.to_column_schema().as_bytes()));
        let schemas = Schemas::from(schemas_bytes);

        let mut data_lengths = LengthsMut::new(cols);

        let mut columns = Vec::new();

        let mut offset = 0;

        for (i, (field, length)) in fields.into_iter().zip(&*lengths).enumerate() {
            macro_rules! _primitive_view {
                ($ty:ident, $prim:ty) => {{
                    debug_assert_eq!(field.bytes(), *length);
                    // column start
                    let start = offset;
                    // column end
                    offset += rows * std::mem::size_of::<$prim>() as usize;
                    // byte slice from start to end: `[start, end)`.
                    let data = bytes.slice(start..offset);
                    // value as target type
                    let value_slice = unsafe {
                        std::slice::from_raw_parts(
                            transmute::<*const u8, *const $prim>(data.as_ptr()),
                            rows,
                        )
                    };
                    // Set data lengths for v3-compatible block.
                    data_lengths[i] = data.len() as u32;

                    // generate nulls bitmap.
                    let nulls = NullsMut::from_bools(
                        value_slice
                            .iter()
                            .map(|v| paste::paste!{ [<$ty:snake _is_null>](v as _) })
                            // .map(|b| *b as u64 == paste::paste! { [<$ty:snake:upper _NULL>] }),
                    )
                    .into_nulls();
                    // build column view
                    let column = paste::paste! { ColumnView::$ty([<$ty View>] { nulls, data }) };
                    columns.push(column);
                }};
            }

            match field.ty() {
                Ty::Null => unreachable!(),

                // Booleans column view.
                Ty::Bool => {
                    debug_assert_eq!(field.bytes(), *length);
                    debug_assert_eq!(field.bytes() as usize, std::mem::size_of::<bool>());

                    let start = offset;
                    // Bool column data end
                    offset += rows; // bool size is 1
                    let data = bytes.slice(start..offset);
                    let nulls = NullsMut::from_bools(data.iter().map(|b| *b as u64 == BOOL_NULL))
                        .into_nulls();

                    data_lengths[i] = data.len() as u32;
                    // build column view
                    let column = ColumnView::Bool(BoolView { nulls, data });
                    columns.push(column);
                }

                // Signed integers columns.
                Ty::TinyInt => _primitive_view!(TinyInt, i8),
                Ty::SmallInt => _primitive_view!(SmallInt, i16),
                Ty::Int => _primitive_view!(Int, i32),
                Ty::BigInt => _primitive_view!(BigInt, i64),
                // Unsigned integers columns.
                Ty::UTinyInt => _primitive_view!(UTinyInt, u8),
                Ty::USmallInt => _primitive_view!(USmallInt, u16),
                Ty::UInt => _primitive_view!(UInt, u32),
                Ty::UBigInt => _primitive_view!(UBigInt, u64),
                // Float columns.
                Ty::Float => _primitive_view!(Float, f32),
                Ty::Double => _primitive_view!(Double, f64),
                Ty::VarChar => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).into_iter().map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = *transmute::<*const u8, *const u16>(ptr);
                        if len == 1 && *ptr.offset(2) == 0xFF {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::VarChar(VarCharView { offsets, data }));

                    data_lengths[i] = *length as u32 * rows as u32;
                }
                Ty::Timestamp => {
                    // column start
                    let start = offset;
                    // column end
                    offset += rows * std::mem::size_of::<i64>() as usize;
                    // byte slice from start to end: `[start, end)`.
                    let data = bytes.slice(start..offset);
                    // value as target type
                    let value_slice = unsafe {
                        std::slice::from_raw_parts(
                            transmute::<*const u8, *const i64>(data.as_ptr()),
                            rows,
                        )
                    };
                    // Set data lengths for v3-compatible block.
                    data_lengths[i] = data.len() as u32;

                    // generate nulls bitmap.
                    let nulls =
                        NullsMut::from_bools(value_slice.iter().map(|b| *b as u64 == BIG_INT_NULL))
                            .into_nulls();
                    // build column view
                    let column = ColumnView::Timestamp(TimestampView {
                        nulls,
                        data,
                        precision,
                    });
                    columns.push(column);
                }
                Ty::NChar => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).into_iter().map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = *transmute::<*const u8, *const u16>(ptr);
                        if len == 4 && *(ptr.offset(2) as *const u32) == 0xFFFFFFFF {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::NChar(NCharView {
                        offsets,
                        data,
                        is_chars: false,
                    }));

                    data_lengths[i] = *length as u32 * rows as u32;
                }
                Ty::Json => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).into_iter().map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = *transmute::<*const u8, *const u16>(ptr);
                        if len == 4 && *(ptr.offset(2) as *const u32) == 0xFFFFFFFF {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(dbg!(ColumnView::Json(JsonView { offsets, data })));

                    data_lengths[i] = *length as u32 * rows as u32;
                }
                Ty::VarBinary => todo!(),
                Ty::Decimal => todo!(),
                Ty::Blob => todo!(),
                Ty::MediumBlob => todo!(),
            }
        }

        Self {
            layout: Layout::INLINE_DEFAULT,
            data: bytes,
            rows,
            cols,
            schemas,
            lengths: data_lengths.into_lengths(),
            precision,
            database: None,
            table: None,
            fields: fields.iter().map(|s| s.name().to_string()).collect(),
            columns,
            group_id: 0,
            raw_fields: Vec::new(),
        }
    }

    pub fn parse_from_raw_block(
        bytes: impl Into<Bytes>,
        rows: usize,
        cols: usize,
        precision: Precision,
    ) -> Self {
        const GROUP_ID_OFFSET: isize = std::mem::size_of::<u32>() as isize;
        const SCHEMA_OFFSET: usize = GROUP_ID_OFFSET as usize + std::mem::size_of::<u64>() as usize;

        let bytes = bytes.into();
        let ptr = bytes.as_ptr();

        let len = unsafe { *(ptr as *const u32) as usize };
        let group_id = unsafe { *(ptr.offset(GROUP_ID_OFFSET) as *const u64) };

        let schema_end = SCHEMA_OFFSET + cols * std::mem::size_of::<ColSchema>();
        let schemas = Schemas::from(bytes.slice(SCHEMA_OFFSET..schema_end));
        // dbg!(&schemas);
        let lengths_end = schema_end + std::mem::size_of::<u32>() * cols;
        let lengths = Lengths::from(bytes.slice(schema_end..lengths_end));
        // dbg!(&lengths);
        let mut data_offset = lengths_end;
        let mut columns = Vec::with_capacity(cols);
        for col in 0..cols {
            // go for each column
            let length = unsafe { *(lengths.deref().get_unchecked(col)) } as usize;
            let schema = unsafe { schemas.get_unchecked(col) };

            macro_rules! _primitive_value {
                ($ty:ident, $prim:ty) => {{
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3); // null bitmap len.
                    data_offset = o2 + rows * std::mem::size_of::<$prim>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::$ty(paste::paste! {[<$ty View>] {
                        nulls: NullBits(nulls),
                        data,
                    }})
                }};
            }

            let column = match schema.ty {
                Ty::Null => unreachable!("raw block does not contains type NULL"),
                Ty::Bool => _primitive_value!(Bool, i8),
                Ty::TinyInt => _primitive_value!(TinyInt, i8),
                Ty::SmallInt => _primitive_value!(SmallInt, i16),
                Ty::Int => _primitive_value!(Int, i32),
                Ty::BigInt => _primitive_value!(BigInt, i64),
                Ty::Float => _primitive_value!(Float, f32),
                Ty::Double => _primitive_value!(Double, f64),
                Ty::VarChar => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;

                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);

                    ColumnView::VarChar(VarCharView { offsets, data })
                }
                Ty::Timestamp => {
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3);
                    data_offset = o2 + rows * std::mem::size_of::<i64>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::Timestamp(TimestampView {
                        nulls: NullBits(nulls),
                        data,
                        precision: precision,
                    })
                }
                Ty::NChar => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;

                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);

                    ColumnView::NChar(NCharView {
                        offsets,
                        data,
                        is_chars: true,
                    })
                }
                Ty::UTinyInt => _primitive_value!(UTinyInt, u8),
                Ty::USmallInt => _primitive_value!(USmallInt, u16),
                Ty::UInt => _primitive_value!(UInt, u32),
                Ty::UBigInt => _primitive_value!(UBigInt, u64),
                Ty::Json => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;

                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);

                    ColumnView::Json(JsonView { offsets, data })
                }
                ty => {
                    unreachable!("unsupported type: {ty}")
                }
            };
            columns.push(column);
            debug_assert!(data_offset <= len);
        }
        // dbg!(&columns);
        RawData {
            layout: Layout::INLINE_DEFAULT,
            data: bytes,
            rows,
            cols,
            precision,
            group_id,
            schemas,
            lengths,
            database: None,
            table: None,
            fields: Vec::new(),
            raw_fields: Vec::new(),
            columns,
        }
    }

    /// Set table name of the block
    pub fn with_database_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.database = Some(name.into());
        self
    }
    /// Set table name of the block
    pub fn with_table_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.table = Some(name.into());
        self
    }

    /// Set fields directly
    pub fn with_fields(&mut self, fields: Vec<Field>) -> &mut Self {
        self.raw_fields = fields;
        self.fields = self
            .raw_fields
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        self
    }

    /// Set field names of the block
    pub fn with_field_names<S: Into<String>, I: Iterator<Item = S>>(
        &mut self,
        names: I,
    ) -> &mut Self {
        self.fields = names.map(|name| name.into()).collect();
        self.raw_fields = self
            .fields
            .iter()
            .zip(self.schemas())
            .map(|(name, schema)| Field::new(name, schema.ty, schema.len))
            .collect();
        self
    }

    fn with_layout(mut self, layout: Layout) -> Self {
        self.layout = layout;
        self
    }

    /// Number of columns
    #[inline]
    pub fn ncols(&self) -> usize {
        self.columns.len()
    }

    /// Number of rows
    #[inline]
    pub fn nrows(&self) -> usize {
        self.rows
    }

    /// Precision for current block.
    #[inline]
    pub const fn precision(&self) -> Precision {
        self.precision
    }

    #[inline]
    pub const fn group_id(&self) -> u64 {
        self.group_id
    }

    #[inline]
    pub fn tmq_table_name(&self) -> Option<&str> {
        self.table.as_ref().map(|s| s.as_str())
    }

    // todo: db name?
    #[inline]
    pub fn tmq_db_name(&self) -> Option<&str> {
        self.database.as_ref().map(|s| s.as_str())
    }

    #[inline]
    pub fn schemas(&self) -> &[ColSchema] {
        &self.schemas
    }

    /// Get field names.
    pub fn field_names(&self) -> &[String] {
        &self.fields
    }

    /// Data view in columns.
    #[inline]
    pub fn columns(&self) -> std::slice::Iter<ColumnView> {
        self.columns.iter()
    }

    /// Data view in rows.
    #[inline]
    pub fn rows<'a>(&self) -> RowsIter<'a> {
        RowsIter {
            raw: NonNull::new(self as *const Self as *mut Self).unwrap(),
            row: 0,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn into_rows<'a>(self) -> IntoRowsIter<'a>
    where
        Self: 'a,
    {
        IntoRowsIter {
            raw: self,
            row: 0,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn deserialize<'de, 'a: 'de, T>(
        &'a self,
    ) -> std::iter::Map<rows::RowsIter<'_>, fn(RowView<'a>) -> Result<T, DeError>>
    where
        T: Deserialize<'de>,
    {
        self.rows().map(|mut row| T::deserialize(&mut row))
    }

    pub fn as_raw_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn is_null(&self, row: usize, col: usize) -> bool {
        if row >= self.nrows() || col >= self.ncols() {
            return true;
        }
        unsafe { self.columns.get_unchecked(col).is_null_unchecked(row) }
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_raw_value_unchecked(
        &self,
        row: usize,
        col: usize,
    ) -> (Ty, u32, *const c_void) {
        let view = self.columns.get_unchecked(col);
        view.get_raw_value_unchecked(row)
    }

    pub fn get_ref(&self, row: usize, col: usize) -> Option<BorrowedValue> {
        if row >= self.nrows() || col >= self.ncols() {
            return None;
        }
        Some(unsafe { self.get_ref_unchecked(row, col) })
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_ref_unchecked(&self, row: usize, col: usize) -> BorrowedValue {
        self.columns.get_unchecked(col).get_ref_unchecked(row)
    }

    unsafe fn get_col_unchecked(&self, col: usize) -> &ColumnView {
        self.columns.get_unchecked(col)
    }

    pub fn to_values(&self) -> Vec<Vec<Value>> {
        self.rows().map(|row| row.into_values()).collect_vec()
    }

    pub fn write<W: std::io::Write>(&self, wtr: W) -> std::io::Result<usize> {
        todo!()
    }
}

// impl BlockExt for RawData {
//     fn num_of_rows(&self) -> usize {
//         self.nrows()
//     }

//     fn fields(&self) -> &[Field] {
//         &self.raw_fields
//     }

//     fn precision(&self) -> Precision {
//         self.precision()
//     }

//     fn is_null(&self, row: usize, col: usize) -> bool {
//         self.is_null(row, col)
//     }

//     unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, BorrowedValue) {
//         (
//             self.get_field_unchecked(col),
//             self.get_ref_unchecked(row, col),
//         )
//     }

//     unsafe fn get_col_unchecked(&self, col: usize) -> &ColumnView {
//         self.get_col_unchecked(col)
//     }
// }

impl Inlinable for RawData {
    fn read_inlined<R: std::io::Read>(reader: R) -> std::io::Result<Self> {
        todo!()
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: W) -> std::io::Result<usize> {
        todo!()
    }
}

#[test]
fn test_block_parser() {
    let rows = 3;
    let cols = 15;
    let precision = Precision::Millisecond;
    static BYTES: &[u8; 460] = b"\xcc\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\t\x00\x08\x00\x00\x00\x01\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x02\x00\x00\x00\x04\x00\x04\x00\x00\x00\x05\x00\x08\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x0c\x00\x02\x00\x00\x00\r\x00\x04\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x06\x00\x04\x00\x00\x00\x07\x00\x08\x00\x00\x00\x08\x00f\x00\x00\x00\n\x00\x92\x01\x00\x00\x0f\x00\x00@\x00\x00\x18\x00\x00\x00\x03\x00\x00\x00\x03\x00\x00\x00\x06\x00\x00\x00\x0c\x00\x00\x00\x18\x00\x00\x00\x03\x00\x00\x00\x06\x00\x00\x00\x0c\x00\x00\x00\x18\x00\x00\x00\x0c\x00\x00\x00\x18\x00\x00\x00\x05\x00\x00\x00\x16\x00\x00\x004\x00\x00\x00\x00?\x8c\xfa\x84\x81\x01\x00\x00>\x8c\xfa\x84\x81\x01\x00\x00?\x8c\xfa\x84\x81\x01\x00\x00\xc0\x00\x00\x01\xc0\x00\x00\xff\xc0\x00\x00\x00\x00\xff\xff\xc0\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xc0\x00\x00\x01\xc0\x00\x00\x00\x00\x01\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x03\x00abc\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x14\x00\x9bm\x00\x00\x1d`\x00\x00\x1e\xd1\x01\x00pe\x00\x00nc\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x1a\x00\x00\x00\x18\x00{\"a\":\"\xe6\xb6\x9b\xe6\x80\x9d\xf0\x9d\x84\x9e\xe6\x95\xb0\xe6\x8d\xae\"}\x18\x00{\"a\":\"\xe6\xb6\x9b\xe6\x80\x9d\xf0\x9d\x84\x9e\xe6\x95\xb0\xe6\x8d\xae\"}\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

    RawData::parse_from_raw_block(Bytes::from_static(BYTES), rows, cols, precision);
}

#[test]
fn test_raw_from_v2() {
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Trace)
        .init();
    use serde::Deserialize;
    let bytes = b"\x10\x86\x1aA \xcc)AB\xc2\x14AZ],A\xa2\x8d$A\x87\xb9%A\xf5~\x0fA\x96\xf7,AY\xee\x17A1|\x15As\x00\x00\x00q\x00\x00\x00s\x00\x00\x00t\x00\x00\x00u\x00\x00\x00t\x00\x00\x00n\x00\x00\x00n\x00\x00\x00n\x00\x00\x00r\x00\x00\x00";

    let block = RawData::parse_from_raw_block_v2(
        bytes.as_slice(),
        &[Field::new("a", Ty::Float, 4), Field::new("b", Ty::Int, 4)],
        &[4, 4],
        10,
        Precision::Millisecond,
    );
    assert!(block.lengths.deref() == &[40, 40]);

    let bytes = include_bytes!("../../../tests/test.txt");

    let block = RawData::parse_from_raw_block_v2(
        bytes.as_slice(),
        &[
            Field::new("ts", Ty::Timestamp, 8),
            Field::new("current", Ty::Float, 4),
            Field::new("voltage", Ty::Int, 4),
            Field::new("phase", Ty::Float, 4),
            Field::new("group_id", Ty::Int, 4),
            Field::new("location", Ty::VarChar, 16),
        ],
        &[8, 4, 4, 4, 4, 18],
        10,
        Precision::Millisecond,
    );

    #[derive(Debug, serde::Deserialize)]
    struct Record {
        ts: String,
        current: f32,
        voltage: i32,
        phase: f32,
        group_id: i32,
        location: String,
    }
    let rows: Vec<Record> = block.deserialize().try_collect().unwrap();
    dbg!(rows);
    // dbg!(block);
}

#[test]
fn test_v2_full() {
    let bytes = include_bytes!("../../../tests/v2.block.gz");

    use flate2::read::GzDecoder;
    use std::io::prelude::*;
    let mut buf = Vec::new();
    let len = GzDecoder::new(&bytes[..]).read_to_end(&mut buf).unwrap();
    assert_eq!(len, 66716);
    let block = RawData::parse_from_raw_block_v2(
        buf,
        &[
            Field::new("ts", Ty::Timestamp, 8),
            Field::new("b1", Ty::Bool, 1),
            Field::new("c8i1", Ty::TinyInt, 1),
            Field::new("c16i1", Ty::SmallInt, 2),
            Field::new("c32i1", Ty::Int, 4),
            Field::new("c64i1", Ty::BigInt, 8),
            Field::new("c8u1", Ty::UTinyInt, 1),
            Field::new("c16u1", Ty::USmallInt, 2),
            Field::new("c32u1", Ty::UInt, 4),
            Field::new("c64u1", Ty::UBigInt, 8),
            Field::new("cb1", Ty::VarChar, 100),
            Field::new("cn1", Ty::NChar, 10),
            Field::new("b2", Ty::Bool, 1),
            Field::new("c8i2", Ty::TinyInt, 1),
            Field::new("c16i2", Ty::SmallInt, 2),
            Field::new("c32i2", Ty::Int, 4),
            Field::new("c64i2", Ty::BigInt, 8),
            Field::new("c8u2", Ty::UTinyInt, 1),
            Field::new("c16u2", Ty::USmallInt, 2),
            Field::new("c32u2", Ty::UInt, 4),
            Field::new("c64u2", Ty::UBigInt, 8),
            Field::new("cb2", Ty::VarChar, 100),
            Field::new("cn2", Ty::NChar, 10),
            Field::new("jt", Ty::Json, 4096),
        ],
        &[
            8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 102, 42, 1, 1, 2, 4, 8, 1, 2, 4, 8, 12, 66, 16387,
        ],
        4,
        Precision::Millisecond,
    );
    dbg!(block);
}

#[test]
fn test_v2_null() {
    let raw = RawData::parse_from_raw_block_v2(
        [0, 0, 0, 0x80].as_slice(),
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let (ty, len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());

    let raw = RawData::parse_from_raw_block_v2(
        [0, 0, 0xf0, 0x7f].as_slice(),
        &[Field::new("a", Ty::Float, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    let (ty, len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    dbg!(raw);
    assert!(null.is_null());
}
#[test]
fn test_from_v2() {
    let raw = RawData::parse_from_raw_block_v2(
        [1].as_slice(),
        &[Field::new("a", Ty::TinyInt, 1)],
        &[1],
        1,
        Precision::Millisecond,
    );
    dbg!(raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawData::parse_from_raw_block_v2(
        [1, 0, 0, 0].as_slice(),
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawData::parse_from_raw_block_v2(
        [2, 0, b'a', b'b'].as_slice(),
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawData::parse_from_raw_block_v2(
        [2, 0, b'a', b'b'].as_slice(),
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let raw = RawData::parse_from_raw_block_v2(
        &[1, 1, 1][..],
        &[
            Field::new("a", Ty::TinyInt, 1),
            Field::new("b", Ty::SmallInt, 2),
        ],
        &[1, 2],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.len(), raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);
    // let v = unsafe { raw.get_ref_unchecked(0, 1) };
    // dbg!(v);
    // let raw = RawBlock::from_v2(
    //     &[1, 2, 0, b'a', b'b'],
    //     &[
    //         Field::new("a", Ty::TinyInt, 1),
    //         Field::new("b", Ty::VarChar, 2),
    //     ],
    //     &[1, 4],
    //     1,
    //     Precision::Millisecond,
    // );
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);
    // let v = unsafe { raw.get_ref_unchecked(0, 1) };
    // dbg!(v);
}

#[test]
fn test_null() {
    let float = unsafe { transmute::<u32, f32>(0x7FF00000) };
    assert!(float.is_nan());
}

#[test]
fn test_bytes() {
    let s = b"abcd";
    let bytes = Bytes::from_static(s);
}
