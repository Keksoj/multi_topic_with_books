use log::{debug, error, info};
use multi_topic_with_books::book_producer::BookProducer;
use pulsar::{executor::Executor, TokioExecutor};
#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let silmarilion: BookProducer<TokioExecutor> = BookProducer::new("silmarilion").await?;

    Ok(())
}
