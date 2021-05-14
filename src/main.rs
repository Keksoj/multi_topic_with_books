use anyhow::Result;
use futures::TryStreamExt;use futures::future::join_all;
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
    let mut book_stream_handles = Vec::new();
    for book_title in BOOK_TITLES.iter() {
        let producer = BookProducer::new(book_title).await?;
        let book_stream_handle = tokio::task::spawn(producer.stream_book());
        book_stream_handles.push(book_stream_handle);
    }

    let book_consumer = BookConsumer::new(&BOOK_TITLES).await?;
    // listen to the topics
    let receive_books_handle = tokio::task::spawn(book_consumer.start());



    // Check for errors by awaiting all tasks
    join_all(book_stream_handles).await;
    receive_books_handle.await??;
    Ok(())
}
