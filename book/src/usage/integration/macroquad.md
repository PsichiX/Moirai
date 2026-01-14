# Macroquad example

Macroquad is the simplest game framework to integrate into, as all you have to do is to create `Jobs` engine, and then you can spawn coroutines (as local jobs) from anywhere in game code.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/macroquad_integration.rs:main}}
```

Also, due to singleton-like nature of Macroquad, when using its scene graph, you can easily access manipulated game objects from anywhere inside Moirai coroutines!

```rust,no_run
{{#rustdoc_include ../../../samples/examples/macroquad_integration.rs:show-dialogue}}
```
