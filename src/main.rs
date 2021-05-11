use anyhow::{Context, Result};
use log::{debug, error, info};
use multi_topic_with_books::book_producer::BookProducer;
use pulsar::{executor::Executor, TokioExecutor};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let silmarilion: BookProducer = BookProducer::new("silmarilion").await?;
    silmarilion.stream_book()?;
    Ok(())
}
