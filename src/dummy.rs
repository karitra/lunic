
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
