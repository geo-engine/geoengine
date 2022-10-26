# Code style

This document collects some best practices we use in our code.

## Expect messages

If you unwrap an `Error` without handling it, you should use `expect` and adhere to the [common messages styles from the Rust Doc](https://doc.rust-lang.org/std/error/index.html#common-message-styles):

> describe the reason we expect the Result should be Ok. With this style we would prefer to write:
> `let path = std::env::var("IMPORTANT_PATH").expect("env variable IMPORTANT_PATH should be set by wrapper_script.sh");`
