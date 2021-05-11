use anyhow::{Context, Result};
use log::{debug, error, info};
use multi_topic_with_books::book_producer::BookProducer;
use pulsar::{executor::Executor, TokioExecutor};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut silmarilion: BookProducer = BookProducer::new("silmarilion").await?;
    let mut bourgeois: BookProducer = BookProducer::new("bourgeois").await?;
    let mut faust: BookProducer = BookProducer::new("faust").await?;

    let send_silmarilion =
        tokio::task::spawn(async move { silmarilion.stream_book().await.unwrap() });
    let send_faust = tokio::task::spawn(async move { faust.stream_book().await.unwrap() });
    let send_bourgeois = tokio::task::spawn(async move { bourgeois.stream_book().await.unwrap() });

    send_silmarilion
        .await
    .context("Could not execute this task")?;
    send_faust.await.context("Could not execute this task")?;
    send_bourgeois.await.context("Could not execute this task")?;

    Ok(())
}
