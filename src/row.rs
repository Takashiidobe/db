use std::num::NonZeroU32;

pub fn to_bytes_bool(b: bool) -> [u8; 1] {
    match b {
        true => [1],
        false => [0],
    }
}

pub fn from_bytes_bool(bytes: &[u8; 1]) -> bool {
    match *bytes {
        [1] => true,
        [0] => false,
        _ => unreachable!(),
    }
}

pub fn from_bytes_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[2..]).to_string()
}

pub fn to_bytes_string(s: &str) -> Vec<u8> {
    let mut res = (s.len() as u16).to_le_bytes().to_vec();

    res.extend(s.bytes());

    res
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowType {
    Id,
    U32,
    Bytes,
    Bool,
}

impl RowType {
    pub fn to_bytes(self) -> [u8; 1] {
        match self {
            RowType::Id => [0],
            RowType::U32 => [1],
            RowType::Bytes => [2],
            RowType::Bool => [3],
        }
    }

    pub fn from_bytes(bytes: &[u8; 1]) -> Self {
        match bytes {
            [0] => RowType::Id,
            [1] => RowType::U32,
            [2] => RowType::Bytes,
            [3] => RowType::Bool,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowVal {
    Id(NonZeroU32),
    U32(u32),
    Bytes(Vec<u8>),
    Bool(bool),
}

impl RowVal {
    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            RowVal::Id(n) => n.get().to_le_bytes().to_vec(),
            RowVal::U32(n) => n.to_le_bytes().to_vec(),
            RowVal::Bytes(b) => {
                let len = b.len() as u16;
                let mut res = len.to_le_bytes().to_vec();
                res.extend(b);
                res
            }
            RowVal::Bool(b) => to_bytes_bool(b).to_vec(),
        }
    }

    pub fn from_bytes(bytes: &[u8], row_type: RowType) -> Self {
        match row_type {
            RowType::U32 => RowVal::U32(u32::from_le_bytes(bytes.try_into().unwrap())),
            RowType::Bytes => {
                let len = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
                RowVal::Bytes(bytes[2..2 + len].to_vec())
            }
            RowType::Bool => RowVal::Bool(from_bytes_bool(bytes.try_into().unwrap())),
            RowType::Id => RowVal::Id(
                u32::from_le_bytes(bytes.try_into().unwrap())
                    .try_into()
                    .unwrap(),
            ),
        }
    }
}

pub fn schema_to_bytes(schema: &[RowType]) -> Vec<u8> {
    let mut res = vec![];
    for row_type in schema {
        res.extend(row_type.to_bytes());
    }
    res
}

pub fn schema_from_bytes(bytes: &[u8]) -> Vec<RowType> {
    let mut res = vec![];
    for byte in bytes.iter().copied() {
        res.push(RowType::from_bytes(&[byte]));
    }
    res
}

pub fn bytes_to_values(bytes: &[u8], schema: &[RowType]) -> (Vec<RowVal>, usize) {
    let mut res = vec![];
    let mut i = 0;

    for row_type in schema {
        match row_type {
            RowType::Id => {
                res.push(RowVal::from_bytes(&bytes[i..i + 4], RowType::Id));
                i += 4;
            }
            RowType::U32 => {
                res.push(RowVal::from_bytes(&bytes[i..i + 4], RowType::U32));
                i += 4;
            }
            RowType::Bytes => {
                let len = u16::from_le_bytes(bytes[i..i + 2].try_into().unwrap()) as usize;
                res.push(RowVal::from_bytes(&bytes[i..i + len + 2], RowType::Bytes));
                i += 2 + len;
            }
            RowType::Bool => {
                res.push(RowVal::from_bytes(&bytes[i..i + 1], RowType::Bool));
                i += 1;
            }
        }
    }

    (res, i)
}

pub fn bytes_to_actions(bytes: &[u8], schema: &[RowType]) -> Vec<Action> {
    let mut res = vec![];
    let mut i = 0;
    // for each set of bytes, we want to increment i by some length and index into it
    while i < bytes.len() - 4 {
        if bytes[i..i + 4] != [0, 0, 0, 0] {
            let (row, incr) = bytes_to_values(bytes, schema);
            res.push(Action::Insert(row));
            i += incr;
        } else {
            let id = bytes_to_id(&bytes[i + 4..i + 8]);
            res.push(Action::Delete(id));
            i += 8;
        }
    }

    res
}

pub fn values_to_bytes(values: &[RowVal]) -> Vec<u8> {
    values.iter().flat_map(|x| x.clone().to_bytes()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    Insert(Vec<RowVal>),
    Delete(NonZeroU32),
}

impl Action {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Action::Insert(row) => row.iter().flat_map(|x| x.clone().to_bytes()).collect(),
            Action::Delete(id) => {
                let mut res = vec![0, 0, 0, 0];
                res.extend(id.get().to_le_bytes());
                res
            }
        }
    }

    pub fn from_bytes(bytes: &[u8], schema: &[RowType]) -> Self {
        match bytes[0..4] {
            [0, 0, 0, 0] => {
                let id = bytes_to_id(&bytes[4..8]);
                Action::Delete(id)
            }
            _ => {
                let rows = bytes_to_values(bytes, schema).0;
                Action::Insert(rows)
            }
        }
    }
}

pub fn bytes_to_id(bytes: &[u8]) -> NonZeroU32 {
    NonZeroU32::new(u32::from_le_bytes(bytes[0..4].try_into().unwrap())).unwrap()
}

pub fn byte_array_to_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut res = u16::to_le_bytes(bytes.len() as u16).to_vec();
    res.extend(bytes);
    res
}

#[cfg(test)]
mod tests {
    use std::num::NonZero;

    use super::*;

    #[test]
    fn serde_string() {
        let s = "example";

        assert_eq!(from_bytes_string(&to_bytes_string(s)), s);
    }

    #[test]
    fn to_wal() {
        let id: NonZeroU32 = NonZero::new(36).unwrap();
        let byte_str = b"example";
        let b = false;
        let n: u32 = 600;

        let vals = vec![
            RowVal::Id(id),
            RowVal::Bytes(byte_str.to_vec()),
            RowVal::Bool(b),
            RowVal::U32(n),
        ];

        let actions = vec![Action::Insert(vals), Action::Delete(1.try_into().unwrap())];

        let action_bytes: Vec<_> = actions.iter().flat_map(|x| x.to_bytes()).collect();
        let schema = &[RowType::Id, RowType::Bytes, RowType::Bool, RowType::U32];

        let deserialized_actions: Vec<_> = bytes_to_actions(&action_bytes, schema);

        assert_eq!(actions, deserialized_actions);
    }

    #[test]
    fn serde_schema() {
        let schema = vec![RowType::Id, RowType::U32, RowType::Bytes, RowType::Bool];

        assert_eq!(schema, schema_from_bytes(&schema_to_bytes(&schema)));
    }

    #[test]
    fn serialize_row() {
        let id: NonZeroU32 = NonZero::new(36).unwrap();
        let byte_str = b"example";
        let b = false;
        let n: u32 = 600;

        let mut bytes = vec![];
        bytes.extend(id.get().to_le_bytes());
        bytes.extend(byte_array_to_bytes(byte_str));
        bytes.extend(to_bytes_bool(b));
        bytes.extend(n.to_le_bytes());

        let schema = [RowType::Id, RowType::Bytes, RowType::Bool, RowType::U32];

        assert_eq!(bytes, values_to_bytes(&bytes_to_values(&bytes, &schema).0));
    }
}
