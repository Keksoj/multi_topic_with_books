# multi_topic_with_books

The goal is to test pulsar's abilities with a bit more data than just "ping" and "pong":
stream three books, each on one topic.

The books, as UTF-8 plain text files:

-   Tolkien's Silmarilion (2300 long lines, 131.000 words)
-   Goethe's Faust (6000 short lines, 31.000 words)
-   Molière's Bourgeois Gentilhomme (3000 long lines, 23.000 words)

A multi-topic consumer will be tasked with reconstructing the books.
