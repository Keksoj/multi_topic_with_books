use crate::{format_topic_for, line::Line, PULSAR_ADDRESS};
use anyhow::{Context, Result};
use futures::Future;
use log::{debug, info};
use pulsar::{
    message::proto,
    producer,
    Pulsar,
    // SubType,
    TokioExecutor,
};
use std::fs::File;
use std::io::{prelude::*, BufReader};

/// This
pub struct BookProducer {
    pub book_title: String,
    pub producer: producer::Producer<TokioExecutor>,
}

impl BookProducer {
    pub async fn new(book_title: &str) -> Result<BookProducer, pulsar::Error> {
        let pulsar = Pulsar::builder(PULSAR_ADDRESS, TokioExecutor)
            .build()
            .await?;

        let topic = format_topic_for(book_title);

        let producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;

        Ok(BookProducer {
            book_title: book_title.to_string(),
            producer,
        })
    }

    pub async fn stream_book(mut self) -> Result<()> {
        let path = format!("books/{}", self.book_title);
        let file = File::open(path).context("Cannot not open file")?;

        let bufreader = BufReader::new(file);
        let mut line_count = 0usize;
        for line in bufreader.lines() {
            let line = Line {
                id: line_count,
                book_title: self.book_title.clone(),
                data: line?.clone(),
            };
            info!("[producer] Sending line: {}", line);
            debug!("[producer] the content: {}", line.data);
            self.producer.send(line).await?;
            line_count += 1;
        }
        Ok(())
    }

    pub fn return_future(self) -> impl Future {
        let future = self.stream_book();
        future
    }
}
