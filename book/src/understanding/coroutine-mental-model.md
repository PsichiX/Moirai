# The coroutine mental model

In this chapter i want to show you what we know from traditional patterns, how it might look like when done using async logic as coroutines.

Do you remember boss fight state machines and the work we needed to do to run them manually to achieve sequential state flow?

Here is how all of that could be expressed as async action timeline:

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:enemy-timeline}}
```

## Action timelines (Coroutines)

Action timelines, or coroutines as game developers tends to call them, are used to describe async logic flow in more sequential (to be more precise: procedural) way, reducing suspension and continuation boundary to single place (`.await`), instead of traditional reactive, scattered and manual state machine management.

> It's worth noting that neihter of those is better than the other - they both express same end goal, but in different ways,  and we shouldn't feel guilty for using one over another.

Coroutines allow constructing such state machines in more procedural way in code, automatically, as suspension and continuation is a first class concept in async, which covers perfectly the declaration and execution part of work that's spread across time, but not across codebase - a state machine that we don't need to handle manually, built for us by the compiler from single code block.

## Automatic state machines

Imagine a scenario where we start with those 3 states we have made for NPC fight pattern, then we want to add a state where NPC can parry incoming player attack while it's in a blocking phase.

Async can provide us with primitives that allow us to easily express "while doing this, you can also try do that other thing" without much of an architectural refactoring, as we would might have need to do with manual state machines.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:enemy-parry}}
```

Instead of just blocking, the enemy will try to parry the player's upcoming attack while it's in blocking phase of a timeline.

> To help us with ergonomic expression of this, we use here an async primitive to run both states at the same time, where blocking state is required to complete, but parry is optional and can be terminated when blocking state completes.

So, as you can see, we didn't had to spend much time on plugging in yet another state, as we did it just in one place, still keeping mental picture of how it fits within general states flow.

## Awaiting game signals

Do you also remember traditional event-based logic flow?

Wanna see how easy would it get to express such construct with coroutines?

Here, let's consider a scenario of quest timeline, where we expect player to go to a couple of places and pick up some stuff:

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_events.rs:timeline}}
```

All we had to do was to declare waiting for certain things to happen in game, and so quest suspends until each step is completed. Being able to declare awaiting for certain game system signal as part of a bigger sequential timeline is quite a benefit!

> Of course in _Big Games ™_, we prefer quests systems to be rather asset-driven, but quests are best at showing in general how to make action timelines awaiting on various game systems triggering events.
