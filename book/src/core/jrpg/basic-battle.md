# Basic battle flow

In this chapter we are gonna show an overview of a text-based turn-based battle flow, as coroutine.

The flow is rather a simple loop conceptually:

- New battle turn
  - Check win conditions of either player or enemy and end battle if either wins.
  - Present information abut player and enemy state to the user.
  - Do player turn.
  - Do enemy turn.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:battle}}
```

> Quick note: `AsyncShared<T>` is equivalent with `Arc<RwLock<T>>` in there to remove a bit of boilerplate. When using async runtimes to run async functions, those that are multithreaded, like Moirai or Tokio, they require data used in there to be `Send` and sometimes also `Sync` to make multithreading work. Of course runtimes can have primitives to run thread-local futures, in that case you can use references to data, but general use is rather multithreaded runtimes.

As you can see in the example, we have easily been able to express entire game loop as simple long-living coroutine.

Now, let's also take a look at one of the other coroutines we are calling in there, a player turn for example (as it will come handy for next chapter):

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:player-turn}}
```

You might be thinking right now, how that compares to traditional game state machine where each step of this is a separate state that handles its suspension and progression to other steps - you might remind yourself how boilerplate-y expressing stuff like that is. And yet with async we have got same behavior, without constructing such state machine ourselves. And we have got it automated, just by having each step as an awaiting future.

I would call it a quite big win, to be honest!
