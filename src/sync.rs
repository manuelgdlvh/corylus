use async_io::Timer;
use event_listener::{Event, listener};
use futures::FutureExt;
use futures::select;

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
        let state = self.state.lock().expect("sync::State inner mutex poisoned");
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
            let mut state = self.state.lock().expect("sync::State inner mutex poisoned");
            *state = value;
        }

        self.cv.notify_all();
    }
}

pub struct AsyncState<T: Default + Eq + PartialEq + Copy + Copy> {
    state: Mutex<T>,
    ev: Event,
}

impl<T: Default + Eq + PartialEq + Copy + Copy> Default for AsyncState<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Default + Eq + PartialEq + Copy + Copy> AsyncState<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(T::default()),
            ev: Event::new(),
        }
    }

    fn check(&self, status: T) -> bool {
        let state = self
            .state
            .lock()
            .expect("sync::AsyncState inner mutex poisoned");
        status.eq(&*state)
    }
    pub async fn await_until(&self, status: T, timeout: Option<Duration>) -> bool {
        if self.check(status) {
            return true;
        }

        listener!(self.ev => listener);
        if self.check(status) {
            return true;
        }

        match timeout {
            None => false,
            Some(val) => {
                let mut delay_fut = Timer::after(val).fuse();
                let mut list_fut = listener.fuse();

                select! {
                    _ = list_fut => {
                    },
                    _ = delay_fut => {
                    },
                }

                self.check(status)
            }
        }
    }

    pub fn update(&self, value: T) {
        {
            let mut state = self
                .state
                .lock()
                .expect("sync::AsyncState inner mutex poisoned");
            *state = value;
        }

        self.ev.notify(usize::MAX);
    }
}
