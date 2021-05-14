use crate::{line::Line, PULSAR_ADDRESS};
use anyhow::{Context, Result};
use futures::TryStreamExt;
use log::{debug, info, trace};

// use log::{debug, error, info};
use pulsar::{
    // message::proto,
    Consumer,
    // DeserializeMessage,
    Pulsar,
    SubType,
    TokioExecutor,
};
// use std::io::{prelude::*, BufReader};

// #[derive(Default)]
pub struct Book {
    title: String,
    lines: Vec<Line>,
}

pub struct BookConsumer {
    pub books: Vec<Book>,
    pub consumer: Consumer<Line, TokioExecutor>,
}

impl BookConsumer {
    pub async fn new<S, I>(topics: I) -> Result<BookConsumer>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let consumer: Consumer<Line, _> = Pulsar::builder(PULSAR_ADDRESS, TokioExecutor)
            .build()
            .await?
            .consumer()
            .with_topics(topics)
            .with_consumer_name("line_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("book_subscription")
            .build()
            .await?;
        Ok(BookConsumer {
            books: Vec::new(),
            consumer,
        })
    }

    pub async fn start(mut self) -> Result<()> {
        while let Some(msg) = self.consumer.try_next().await? {
            trace!("metadata: {:?}", msg.metadata());
            let line = msg.deserialize()?;
            self.consumer.ack(&msg).await?;
            debug!("[consumer] received a line: {}", line);
            if line.id % 500 == 0 {
                info!("Received {} lines of {}", line.id, line.book_title);
            }
            self.process_line(line);
        }
        Ok(())
    }

    pub fn process_line(&mut self, line: Line) {
        // clone the title to have it available through all checks
        let title = line.book_title.clone();

        // if the book is already present
        for book in self.books.iter_mut() {
            if book.title == title {
                book.lines.push(line);
                return;
            }
        }

        // Else, create a new book
        let mut lines = Vec::new();
        lines.push(line);
        self.books.push(Book { title, lines });
    }
}
