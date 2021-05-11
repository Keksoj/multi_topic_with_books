pub mod book_producer;
pub mod book_consumer;
pub mod line;

pub const PULSAR_ADDRESS: &'static str = "pulsar://127.0.0.1:6650";

pub fn format_topic_for<S: Into<String>>(book_title: S) -> String {
    format!("persistent://public/default/{}", book_title.into())
}
