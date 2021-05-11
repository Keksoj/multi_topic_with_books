use crate::{line::Line, PULSAR_ADDRESS};
use anyhow::{Context, Result};
use log::{debug, error, info};
use pulsar::{
    executor::Executor, message::proto, producer, Consumer, DeserializeMessage, Pulsar, SubType,
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

        let producer = pulsar
            .producer()
            .with_topic(book_title)
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
        let mut file = File::open(path).context("Cannot not open file")?;

        // let mut contents = String::new();
        // file.read_to_string(&mut contents)?;
        // println!("{}", contents);

        let mut bufreader = BufReader::new(file);
        let mut line_count = 0usize;
        for line in bufreader.lines() {
            let line = line?;
            self.producer
                .send(Line {
                    id: line_count,
                    data: line,
                })
                .await?;
            info!("[{}] Sent line #{}", self.book_title, line_count);
            line_count += 1;
        }
        Ok(())
    }
}
