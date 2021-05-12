use anyhow::Result;
use futures::TryStreamExt;
use log::{debug, info};
use multi_topic_with_books::{
    book_consumer::BookConsumer,
    book_producer::BookProducer,
    // format_topic_for, line::Line, PULSAR_ADDRESS,
    BOOK_TITLES,
};
// use pulsar::{executor::Executor, Consumer, Pulsar, SubType, TokioExecutor};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // create book producers and futures
    // let mut futures = Vec::new();
    for book_title in BOOK_TITLES.iter() {
        let producer = BookProducer::new(book_title).await?;
        let future = producer.stream_book();
        future.await?;
    }

    let mut book_consumer = BookConsumer::new(&BOOK_TITLES).await?;
    // listen to the topics
    while let Some(msg) = book_consumer.consumer.try_next().await? {
        debug!("metadata: {:?}", msg.metadata());
        let line = msg.deserialize()?;
        book_consumer.consumer.ack(&msg).await?;
        info!("[consumer] received a line: {}", line);
        book_consumer.process_line(line);
    }
    Ok(())
}
