# geo engine
 This workspace contains the geo engine crates.

# Development

## Lints
Please run Clippy with 
`cargo clippy --all-targets --all-features`
before creating a pull request.

## Testing
Please provide tests with all new features and run
`cargo test`
before creating a pull request.

## Benchmarks
For performance-critical features, we aim to provide benchmarks in the `benches` directory.
If you plan on optimizing a feature of Geo Engine, please confirm it this way.
