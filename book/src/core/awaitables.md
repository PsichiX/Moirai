# Awaitables and timeline thinking

## What is awaitable

Awaitable is a point, where **logic intentionally stops until _X_ happen**. It's a contract with the engine that tells: "wake me up when requirement for progression gets fulfilled".

Take a look at this coroutine:

```rust,no_run
async {
    wait_for_input_action("interact").await;
    wait_for_seconds(0.1).await;
    wait_for_animation_to_finish("punch").await;
}
```

Every `.await` in there is that point, which suspends coroutine until future we await is complete.

> It's important to remember: **awaiting doesn't mean sleeping** - it means yielding control.

When we await at `wait_for_seconds()`, we are _yielding control_ to other coroutines running in the background, until timer hits the elapsed time we requested, and only then coroutine moves forward to the next awaiting point.

We are suspending coroutine, no code is executed at all until coroutine gets told that whatever we wait for is done and available, and only then execution does continue further - nothing happen until then, other coroutines can do their work at the same time until they hit await point.

This distinction is important, as async might at first feel like a magic, and it might bring up wrong intuition for gamedevs that are used to just spawn task in a thread and `sleep()` in it to wait for some time, or in case of something like `wait_for_input_action()`, that it just blocking-loop constantly asking if input action did triggered - that's wrong way to look at it, it just pauses and lets game do other work while it waits.

## What are action timelines

Action timeline is a concept, where you lay out a sequence of state changes spread in time.

In traditional way, you can see action timeline defined within our NPC fight pattern as:

```rust,no_run
{{#rustdoc_include ../../samples/examples/traditional_state_machines.rs:fight-pattern}}
```

In traditional way this is how we tell the manually driven state machine what is expected chain of actions, and state machine takes care of switching to the next action, when current action completes. In there we declare it in single place, so it could help us keep mental picture of the logic flow.

> You can also think of action timelines as scripting a sequence of steps, where all steps doesn't happen _now_, but each does _sometime in the future_, if that helps you.

And similarily to traditional way, we can declare such timeline of actions as a coroutine, in form of a procedurally looking code, denoting with await points where the transition happen, explicitly telling we are awaiting for each step completion.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:enemy-timeline}}
```

With difference that we don't need to care about any state machine running the states and transitions, as it's built for us by the compiler - we get a coroutine with action timeline, send it for execution and do something useful with its result when it's done.

## Key points on more complex example

To summarize, looking at this code, slightly more complex than what we have shown so far:

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:player-timeline}}
```

1. At every `.await` point, this coroutine **suspends** - it does not block current thread, does not sleep. It just suspends execution entirely, until requested resource is available (like new batch of `events`, that might be available immediatelly or soon).
1. Each branch of `if` statement represents a selection of action sub-timeline, meaning whether we choose to do `attack()`, `block()` or `update()` state, we will suspend entire coroutine, loop itself, until selected state completes, then after resuming we _might_ then wait for next game frame to occur and repeat.
1. Every loop iteration here represents a _game frame or more_ (if we are currently executing any of above states; explained _why_ later).

I get that this specific coroutine might look weird, as we see a loop waiting for next frame at the end. It might give us thinking that single iteration is single game frame, but then we suddenly await inside that iteration? This is confusing!

It might not be intuitive at first, i really get you. But bare with me for a second.

What if that `next_frame()` await in loop is actually the single point of confusion? Here, look at this:

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:waiting-for-input}}
```

If we remove it, then this loop reads:

> Acquire events -> Decide on which state user wants to be next -> Do selected state -> Repeat

So why we have put that `next_frame()` in the first place?

Well, because what if all async functions called in there would complete immediatelly? It can happen, if given function depends only on resource availability, and it happen that resource is available upfront.

In that case we don't want to loop for ever, blocking entire game until some of async functions won't complete immediatelly, so we just expect given iteration to wait for next frame for good measure.
