use std::fmt;

pub trait Logger: Send + Sync + Clone + 'static {
    fn info(&self, message: fmt::Arguments<'_>);
    fn debug(&self, message: fmt::Arguments<'_>);
    fn trace(&self, message: fmt::Arguments<'_>);
    fn warn(&self, message: fmt::Arguments<'_>);
    fn error(&self, message: fmt::Arguments<'_>);
}
