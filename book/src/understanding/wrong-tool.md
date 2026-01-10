# When coroutines are the wrong tool

While coroutines might seem like a nice way to organize your suspendable state machines in procedural way, they aren't meant to fit all problems. In fact, they can even worsen your architecture if applied to wrong problems, and then here comes spaghetti in just another flavor!

> Let's make sure we do understand first, that _coroutines describe **when things happen**_, while _general game systems describe **what is always happening**_.

Now, let's talk about where they fit and where they don't, to gain some intuition on deciding when and where to use them.

## Where coroutines **doesn't** belong

### Per-frame simulation logic

Generally things that must happen every frame with immediately applied side effects, such as for example:

- Movement integration.
- Physics.
- Animation blending.
- Game and camera controls.
- Rendering and audio.

And so on. Those things belong to proper per-frame systems, where there is no suspension and continuation needed or not even encouraged to model as an async actions timeline.

Don't even try expressing your entire game loop frame with silly:

```rust,no_run
async {
    loop {
        player_movement_system();
        physics_system();
        ai_system();
        render_system();

        next_frame().await;
    }
}
```

As this would be just a confusing and overdone way to do what regular proper frame iteration method on a game state would do.

> If your coroutine logic suspends only on frame end, you just added hidden state machine with useless suspension for no good reason.

### Immediate computation

Most of the game logically is usually computation and data transformation with optional side effects. In there, there is usually zero need for suspension outside of special cases, and so another silly thing would be to just do things like:

```rust,no_run
async fn increment_until_10(mut value: i32) {
    while value < 10 {
        value += 1;
        next_frame().await;
    }
}
```

As we don't wait for some specific game state being available, we are here incrementing some value and no side effect happen that needs to be suspendable - we could just compute the expected state with no artificial suspension baked in and be fine.

> If your function starts and ends at the same time, don't bother making it a coroutine.

### Async doesn't mean simpler by default

Coroutines hide state, they don't eliminate it. Poorly designed await chains are just delayed spaghetti, that you wanted to avoid.

Coroutines code at the async function building blocks level may also end up ugly, depending on what you're doing in them. Take a look at this state function:

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:charge}}
```

What makes it ugly is that `this.write().unwrap()` accessor, which is needed for accessing data that's essentially smart pointers, in order to make async compilation happy about lifetime of objects and their mutability (more about it in later chapters).

> Honestly, this is my personal grudge towards async in Rust, that you will need to use smart pointers, accessed especially like that, because if you operate on shared state instead of moving data in and out of future, you would want to access said data also outside of coroutine in per-frame game systems. So no prospect for win here.
>
> This is not a problem of async itself, but comes from requirements of multithreaded async runtimes, so in the end when you _need_ shared state (instead of sending it around), you _need_ smart pointers, and therefore your code ends up usually uglier.
>
> This doesn't happen when you use singlethreded runtimes, but that has less coverage on average, so..

The only redeeming value here is that this seeming uglyness is hidden from the user most of the times, as user mostly just calls this function in a timeline, not necessarily needing to dive into its code to figure out the state flow, as states flow is directed at the caller location, not in the function.

### Debugging is **_HARD_**

I can't stress this enough, but debugging async functions is much harder than debugging regular functions, as your callstack will get fragmented locations not showing the origin of an async function, but most of the times showing the polling executor site, with bit of an async function where current poll happen.

This makes entire experience much harder than it should be. Even if stepping with a debugger nowadays is somewhat reasonable and works in debuggers, the stack, that should tell you how you got into that place in code, tells you nothing useful at all.

So, before you jump into making some part of game logic a coroutine, take that into consideration, and evaluate if expressing suspendable logic in a tidy manner is more important to you than being able to debug why something went wrong in there, with unsane lack of useful information.

### Serializing is **_Super HARD_**

**Till this day there is not much tools for serializing running coroutines** (or any async tasks in that matter).

Some runtimes try to provide some means to add durability of async tasks, at least partially, but usually you are better off with not relying on these mechanisms, and instead making coroutine rely on serializable state it operates on, so game can serialize that state outside of coroutine, and not any state of a coroutine.

For example take enemy fight pattern timeline:

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_mental_model_states.rs:enemy-timeline}}
```

There is no state exclusive to this coroutine that needs to be serialized, as it operates purely on game characters that will be serialized by game.

The only thing we would like to serialize here would be a point between which states enemy is in its fight pattern.

> Although games usually save game at safe checkpoints, outside of battles, because that's another category of problems in general, whether we do sync or async game logic.

## Where coroutines **do** belong

Coroutines have small, but rather useful set of scenarios to be used in, replacing manual state machines with automatic ones there.

They are great when:

- Logic spans across multiple frames or time.
- You wait for resources being available or external events to happen.
- The flow is naturally linear.

Which makes sense for long lived tasks, such as:

- **Cutscenes**

    Triggering some related game state changes, awaiting for when that state change completes to move to next step.

- **Tutorials**

    Orchestrating in order tooltips to show on screen, awaiting user inputs.

- _Sometimes_ **AI action patterns**

    Where player should be able to learn them in order to anticipate NPC's next move for advantage. It gets less useful the more AI is asset-driven, then coroutines might end up actually worsening the readability, but YMMV.

- _Sometimes_ **Quests**

    Where player is expected to do concrete actions, and/or specific game events should happen. But just like with AI action patterns, this also makes coroutines less useful the more asset-driven quests system is.

- **Scripted behaviors**

    This is something between cutscenes and quests, where we might have some NPC AI paused for short duration, doing a very custom behavior, which wouldn't be easily produced with pure AI state machine.

    From my experience i can say, that the more complex AI could get, the more problematic it gets to bake scripted behavior into the AI system and so we might wanna prefer to disable NPC's AI entirely and _Do The Thing ™_, then get back to AI handling its usual self.

- **Spreading compute-heavy logic across frames**

    This applies generally to game initialization or non-blocking loading, as we would rather wanna show loading screen not jittering every second, or bluntly freezing game window until completes.

    Let's say we do some procedural scene generation for your roguelike, which usually involves quite intense computation, the bigger the world it generates is, for example using Wave Function Collapse algorithm - we don't want to stall any game frame for more than couple of milliseconds top, so we might wanna identify useful points in PCG logic, where suspending until next game frame will come beneficial to smoothen user experience. Let's make it clear: this is not a _Skill Issue_ problem, it's generally a hard problem to make ergonomic scripted actions subsystem of AI system, where there is as many solutions as there is number of systems.

And scenarios similar to above, that i didn't mentioned, because list could go on and show very little difference between next positions.

---

If i want you to take anything from this chapter, it should definitely be:

> **Coroutines don't replace systems - they only tidy gluecode between their parts.**
