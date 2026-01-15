list:
  just --list

format:
  cargo fmt --all

build:
  cargo build --all --all-features

test:
  cargo test --all --all-features -- --nocapture

miri:
  cargo +nightly miri test --manifest-path ./crates/_/Cargo.toml -- --nocapture
  # TODO: Fix miri errors in ManagedBox lazy access - stacked borrows errors.
  # cargo +nightly miri test --manifest-path ./crates/durable/Cargo.toml -- --nocapture
  
clippy:
  cargo clippy --all --all-features
  cargo clippy --tests --all --all-features

checks:
  just format
  just build
  just clippy
  just test
  just miri

clean:
  find . -name target -type d -exec rm -r {} +
  just remove-lockfiles

remove-lockfiles:
  find . -name Cargo.lock -type f -exec rm {} +

list-outdated:
  cargo outdated -R -w

update:
  cargo update --manifest-path ./crates/_/Cargo.toml --aggressive
  cargo update --manifest-path ./crates/durable/Cargo.toml --aggressive
  cargo update --manifest-path ./crates/bevy/Cargo.toml --aggressive

book:
  mdbook build book

book-dev:
  mdbook watch book --open

publish:
  cargo publish --no-verify --manifest-path ./crates/_/Cargo.toml
  sleep 1
  cargo publish --no-verify --manifest-path ./crates/durable/Cargo.toml
  sleep 1
  cargo publish --no-verify --manifest-path ./crates/bevy/Cargo.toml
