# Prompts

Prompts are kinds of coroutines where we have to await on specific user input.

Let's remind ourselves the player turn:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:player-turn}}
```

There are two things interesting us:

- `wait_for_key()`
- `selected_action()`

Waiting for turn requires us to check for events if there is a specific key pressed event.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:wait-for-key}}
```

While awaiting for user selected action is bit more complex, as it interacts with the user input and UI widget to fetch data from the UI options list.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:selected-action}}
```

And this is how our synchronous side manages user-selected action provided to running coroutines:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:take-action-screen-update}}
```

This method of a `TakeActionScreen` is then called every game frame.

Effectively, we have showed shared state communication between traditional game logic and running coroutines.
