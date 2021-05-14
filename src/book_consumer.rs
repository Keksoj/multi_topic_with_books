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
    finished: bool,
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
        info!("[consumer] Starting consuming topics…");

        while let Some(msg) = self.consumer.try_next().await? {
            trace!("metadata: {:?}", msg.metadata());
            let line = msg.deserialize()?;
            self.consumer.ack(&msg).await?;
            debug!("[consumer] received a line: {}", line);
            if line.id % 500 == 0 {
                info!("Received {} lines of {}", line.id, line.book_title);
            }

            if self.all_books_are_finished() {
                self.finishing_statement();
                return Ok(());
            }

            self.process_line(line);
        }
        Ok(())
    }

    fn all_books_are_finished(&self) -> bool {
        if self.books.len() == 0 {
            return false;
        }
        for book in self.books.iter() {
            if !book.finished {
                return false;
            }
        }
        info!("[consumer] all books are finished!");

        true
    }

    fn finishing_statement(&self) {
        for book in self.books.iter() {
            info!("{} has {} lines", book.title, book.lines.len());
        }
    }

    pub fn process_line(&mut self, line: Line) {
        // clone the title to have it available through all checks
        let title = line.book_title.clone();

        // if the book is already present
        for book in self.books.iter_mut() {
            if book.title == title {
                if line.is_last_line() {
                    book.finished = true;
                    return;
                }

                book.lines.push(line);
                return;
            }
        }

        // Else, create a new book
        let mut lines = Vec::new();
        lines.push(line);
        self.books.push(Book {
            title,
            lines,
            finished: false,
        });
    }
}
