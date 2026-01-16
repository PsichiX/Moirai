# Bevy example

Bevy has it's own dedicated Moirai plugin, that handles all required integration.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/bevy_integration.rs:main}}
```

So we can easily just grab `Coroutines` resource and spawn jobs.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/bevy_integration.rs:setup}}
```

For world manipulation, we can access entire ECS world.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/bevy_integration.rs:show-dialogue}}
```

The cutscene coroutine we run looks like this:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/bevy_integration.rs:cutscene}}
```
