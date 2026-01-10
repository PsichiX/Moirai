# Quick start

I see you're curious of how coroutines looks like in Rust - let's feed you some minimal example!

## Installation

Add Moirai to the project:

```bash
cargo add moirai
```

## Minimal code

First we create jobs runtime to spawn coroutines into:

```rust,no_run
{{#rustdoc_include ../samples/examples/moirai_minimal.rs:setup}}
```

Then we spawn some job somewhere in game:

```rust,no_run
{{#rustdoc_include ../samples/examples/moirai_minimal.rs:spawn-job}}
```

In game loop we run pending local jobs queue, usually at the end of game frame:

```rust,no_run
{{#rustdoc_include ../samples/examples/moirai_minimal.rs:game-frame}}
```

As you see, this is bare minimum needed to process coroutines in your game. Just a runtime running local jobs at the end of a game frame, and a spawned jobs in it - that's all!
