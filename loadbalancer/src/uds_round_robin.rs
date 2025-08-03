use std::sync::Mutex;

pub struct UdsRoundRobin {
    paths: Vec<String>,
    counter: Mutex<usize>,
}

impl UdsRoundRobin {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            paths,
            counter: Mutex::new(0),
        }
    }

    pub fn select(&self) -> Option<String> {
        let mut counter = self.counter.lock().unwrap();
        if self.paths.is_empty() {
            return None;
        }
        let path = self.paths[*counter % self.paths.len()].clone();
        *counter += 1;
        Some(path)
    }
}
