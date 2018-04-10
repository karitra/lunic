use std::collections::HashMap;

pub type DummyTable = HashMap<String, DummyRecord>;


#[derive(Debug, Serialize, Deserialize)]
pub struct DummyRecord {
    id: i32,
    y:  i32,
    z:  String,
}

impl DummyRecord {
    pub fn new(id: i32) -> DummyRecord {
        DummyRecord{
            id, y: 2, z: String::from("a"),
        }
    }
}

pub fn make_table(size: u32) -> DummyTable {
    let mut init_data = HashMap::new();

    for i in 0..size {
        init_data.insert(format!("some_{}", i), DummyRecord::new(i as i32));
    }

    init_data
}
