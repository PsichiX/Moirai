# General game loop model

Moirai, when using it's ready-to-go `Jobs` engine, it's super easy to plug into your game, as all what you do is you create `Jobs` engine and store it in your game/engine resources.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_integration.rs:main}}
```

Default `Jobs` engine creates worker-threads for each CPU core. If you need only local jobs, like many games do for coroutines, you can always use `Jobs::local_only()`.
