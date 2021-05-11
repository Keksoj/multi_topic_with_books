use anyhow::{Context, Result};
use futures::TryStreamExt;
use log::{debug, error, info};
use multi_topic_with_books::{
    book_consumer::BookConsumer,
    book_producer::BookProducer, format_topic_for, line::Line, PULSAR_ADDRESS,
};
use pulsar::{executor::Executor, Consumer, Pulsar, SubType, TokioExecutor};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut topics = Vec::new();
    topics.push(format_topic_for("silmarilion"));
    topics.push(format_topic_for("faust"));
    topics.push(format_topic_for("bourgeois"));

    let mut book_consumer = BookConsumer::new(topics).await?;

    // book producers
    let silmarilion: BookProducer = BookProducer::new("silmarilion").await?;
    let bourgeois: BookProducer = BookProducer::new("bourgeois").await?;
    let faust: BookProducer = BookProducer::new("faust").await?;

    let send_silmarilion =
        tokio::task::spawn(async move { silmarilion.stream_book().await.unwrap() });
    let send_faust = tokio::task::spawn(async move { faust.stream_book().await.unwrap() });
    let send_bourgeois = tokio::task::spawn(async move { bourgeois.stream_book().await.unwrap() });

    send_silmarilion
        .await
        .context("Could not execute this task")?;
    send_faust.await.context("Could not execute this task")?;
    send_bourgeois
        .await
        .context("Could not execute this task")?;

    // listen to the topics
    while let Some(msg) = book_consumer.consumer.try_next().await? {
        debug!("metadata: {:?}", msg.metadata());
        let line = msg.deserialize()?;
        book_consumer.consumer.ack(&msg).await?;
    }
    Ok(())
}
