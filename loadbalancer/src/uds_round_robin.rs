use std::sync::atomic::{AtomicUsize, Ordering};

pub struct UdsRoundRobin {
    paths: Vec<String>,
    counter: AtomicUsize,
}

impl UdsRoundRobin {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            paths,
            counter: AtomicUsize::new(0),
        }
    }

    pub fn select(&self) -> Option<String> {
        if self.paths.is_empty() {
            return None;
        }
        let current = self.counter.fetch_add(1, Ordering::Relaxed);
        Some(self.paths[current % self.paths.len()].clone())
    }
}
