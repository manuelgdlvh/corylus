use std::sync::{Condvar, Mutex};
use std::time::Duration;

pub struct State<T: Default + Eq + PartialEq + Copy + Copy> {
    state: Mutex<T>,
    cv: Condvar,
}

impl<T: Default + Eq + PartialEq + Copy + Copy> Default for State<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Default + Eq + PartialEq + Copy + Copy> State<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(T::default()),
            cv: Condvar::new(),
        }
    }

    pub fn await_until(&self, status: T, timeout: Option<Duration>) -> bool {
        let state = self.state.lock().expect("Cannot be poisoned");
        if status.eq(&*state) {
            return true;
        }

        match timeout {
            Some(val) => match self.cv.wait_timeout(state, val) {
                Ok((state, res)) => !res.timed_out() && status.eq(&*state),
                Err(_) => false,
            },
            None => false,
        }
    }

    pub fn update(&self, value: T) {
        {
            let mut state = self.state.lock().expect("Cannot be poisoned");
            *state = value;
        }

        self.cv.notify_all();
    }
}
