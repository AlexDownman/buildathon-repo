//! Minimal Parquet writer: one row group, one PLAIN uncompressed v1 data page
//! per column, Thrift compact metadata written by hand. Only what the two
//! output tables need: REQUIRED INT32 (DATE), REQUIRED INT64, REQUIRED and
//! OPTIONAL BYTE_ARRAY (STRING).

use std::io::Write;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Str,
    OptStr,
    Date,
    I64,
}

pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    /// PLAIN-encoded values (non-null only for OptStr)
    pub data: Vec<u8>,
    /// OptStr only: one bit per row, 1 = present
    pub present: Vec<bool>,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, rows: usize) -> Column {
        let per = match kind {
            Kind::Date => 4,
            Kind::I64 => 8,
            _ => 16,
        };
        Column { name, kind, data: Vec::with_capacity(rows * per), present: Vec::new() }
    }
    #[inline]
    pub fn push_str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
    }
    #[inline]
    pub fn push_opt_str(&mut self, s: Option<&[u8]>) {
        self.present.push(s.is_some());
        if let Some(s) = s {
            self.push_str(s);
        }
    }
    #[inline]
    pub fn push_i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn push_i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }
}

// ---- Thrift compact protocol ----

struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BIN: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

impl T {
    fn new() -> T {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        loop {
            if v < 0x80 {
                self.b.push(v as u8);
                return;
            }
            self.b.push((v as u8 & 0x7F) | 0x80);
            v >>= 7;
        }
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, CT_I32);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, CT_I64);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &str) {
        self.field(id, CT_BIN);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
    fn begin_struct_field(&mut self, id: i16) {
        self.field(id, CT_STRUCT);
        self.last.push(0);
    }
    fn begin_struct_elem(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list_header(&mut self, id: i16, ety: u8, n: usize) {
        self.field(id, CT_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
}

const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;

fn ptype(k: Kind) -> i32 {
    match k {
        Kind::Str | Kind::OptStr => T_BYTE_ARRAY,
        Kind::Date => T_INT32,
        Kind::I64 => T_INT64,
    }
}

/// RLE/bit-packed hybrid, bit width 1, as one bit-packed run, with the
/// 4-byte length prefix used by v1 data pages.
fn def_levels(present: &[bool]) -> Vec<u8> {
    let groups = present.len().div_ceil(8);
    let mut body = Vec::with_capacity(groups + 8);
    let mut h = ((groups as u64) << 1) | 1;
    loop {
        if h < 0x80 {
            body.push(h as u8);
            break;
        }
        body.push((h as u8 & 0x7F) | 0x80);
        h >>= 7;
    }
    for g in 0..groups {
        let mut byte = 0u8;
        for i in 0..8 {
            let j = g * 8 + i;
            if j < present.len() && present[j] {
                byte |= 1 << i;
            }
        }
        body.push(byte);
    }
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

pub fn write(path: &std::path::Path, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::with_capacity(4 << 20, std::fs::File::create(path)?);
    f.write_all(b"PAR1")?;
    let mut off: u64 = 4;
    struct Meta {
        offset: u64,
        size: u64,
    }
    let mut metas = Vec::new();
    for c in cols {
        let levels = if c.kind == Kind::OptStr { def_levels(&c.present) } else { Vec::new() };
        let body_len = levels.len() + c.data.len();
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, body_len as i32);
        h.i32f(3, body_len as i32);
        h.begin_struct_field(5);
        h.i32f(1, rows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.end_struct();
        h.b.push(0);
        f.write_all(&h.b)?;
        f.write_all(&levels)?;
        f.write_all(&c.data)?;
        let size = (h.b.len() + body_len) as u64;
        metas.push(Meta { offset: off, size });
        off += size;
    }
    // FileMetaData
    let mut t = T::new();
    t.i32f(1, 1);
    t.list_header(2, CT_STRUCT, cols.len() + 1);
    t.begin_struct_elem();
    t.strf(4, "schema");
    t.i32f(5, cols.len() as i32);
    t.end_struct();
    for c in cols {
        t.begin_struct_elem();
        t.i32f(1, ptype(c.kind));
        t.i32f(3, if c.kind == Kind::OptStr { 1 } else { 0 });
        t.strf(4, c.name);
        match c.kind {
            Kind::Str | Kind::OptStr => {
                t.i32f(6, 0); // UTF8
                t.begin_struct_field(10);
                t.begin_struct_field(1); // STRING
                t.end_struct();
                t.end_struct();
            }
            Kind::Date => {
                t.i32f(6, 6); // DATE
                t.begin_struct_field(10);
                t.begin_struct_field(6); // DATE
                t.end_struct();
                t.end_struct();
            }
            Kind::I64 => {}
        }
        t.end_struct();
    }
    t.i64f(3, rows as i64);
    t.list_header(4, CT_STRUCT, 1);
    t.begin_struct_elem();
    t.list_header(1, CT_STRUCT, cols.len());
    let mut total = 0u64;
    for (c, m) in cols.iter().zip(metas.iter()) {
        total += m.size;
        t.begin_struct_elem();
        t.i64f(2, m.offset as i64);
        t.begin_struct_field(3);
        t.i32f(1, ptype(c.kind));
        t.list_header(2, CT_I32, 2);
        t.zz(0);
        t.zz(3);
        t.list_header(3, CT_BIN, 1);
        t.varint(c.name.len() as u64);
        t.b.extend_from_slice(c.name.as_bytes());
        t.i32f(4, 0); // UNCOMPRESSED
        t.i64f(5, rows as i64);
        t.i64f(6, m.size as i64);
        t.i64f(7, m.size as i64);
        t.i64f(9, m.offset as i64);
        t.end_struct();
        t.end_struct();
    }
    t.i64f(2, total as i64);
    t.i64f(3, rows as i64);
    t.end_struct();
    t.strf(6, "shopping-sessions");
    t.b.push(0);
    f.write_all(&t.b)?;
    f.write_all(&(t.b.len() as u32).to_le_bytes())?;
    f.write_all(b"PAR1")?;
    f.flush()?;
    Ok(())
}
