use std::io;

#[derive(Clone)]
pub struct Options {
    pub name: String,
    pub threads: usize,
}

pub trait Builder {
    type Runtime: Runtime;
    fn build(&self, opts: Options) -> io::Result<Self::Runtime>;
}

pub trait Runtime {
    fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static;
}
