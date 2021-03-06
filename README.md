# [Pulsar] multi_topic_with_books

The goal is to test pulsar's abilities with a bit more data than just "ping" and "pong".
Let's stream three books on three topics.

The books are UTF-8 plain text files:

-   Tolkien's Silmarilion (2300 long lines, 131.000 words)
-   Goethe's Faust (6000 short lines, 31.000 words)
-   Molière's Bourgeois Gentilhomme (3000 long lines, 23.000 words)

A multi-topic consumer will be tasked with reconstructing the books.

## How to run

Be sure to have a Pulsar server running as standalone on your machine, following
[this guide](https://pulsar.apache.org/docs/en/standalone/). To sum up, once you've downloaded the binary, go into the directory and run

    bin/pulsar standalone

Then run the project with

    RUST_LOG=info cargo run

## What I learned

You can start an asynchronous function by spawning a task like so:

```rust
let handle = tokio::task::spawn(some_async_function_that_returns_a_result());
```

The job works and you can call other functions.
You can handle the result at the bottom of `main()`:

```rust
handle.await??;
```
