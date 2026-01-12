# Helpful coroutine primitives

While building suspendable game logic, it's great to build from smaller building blocks. We are about to list most helpful blocks every game-oriented async runtime should provide in one way or another, in order to let us not care much about internal work of such blocks.

## Yielding control

> You know them, you love them - `yield_now()`.

This is the core of every runtime building blocks, as it does nothing more than just tells runtime that your async job can just pause and let other coroutines continue their part of work. Without it, coroutines could block game frame for longer than it needs to, therefore consuming precious frame time that's required the most for game systems.

```rust,no_run
async {
    for index in 0..10 {
        println!("Iteration: {index}");
        yield_now().await;
    }
}
```

## Time-based waiting

> - `wait_for_next_tick()`.
> - `wait_ticks(<N>)`.
> - `wait_duration(<TIME>)`.

They won't progress until specific time or frame counter increases enough times. Useful for describing explicit deferral of work in game time, like cutscenes, scripted behavior or splitting heavy computation for example.

```rust,no_run
async {
    for step in 0..10 {
        println!("Step: {index}");
        wait_duration(Duration::from_secs(1)).await;
    }
}
```

## Event-based waiting

> - `wait_for_message(<RECEIVER>)`.
> - `wait_for_notification(<NOTIFIER>)`.

They expect to progress only after certain signal from game systems or other async work gets received. Useful especially for communication between parts of the game, like quests or entire UI action chains (for example: connecting to multiplayer game in main menu).

```rust,no_run
async {
    while let Some(message) = wait_for_message(&game_events).await {
        message.execute();
    }
}
```

## Spawning async work

> The bread and butter of concurrency - `spawn(<ASYNC WORK>)`.

It allows to spawn new async work as coroutine, making it run in the background, being polled at specific point in a game frame, or in another thread when using multithreaded runtimes.

```rust,no_run
async fn move_towards(object: GameObject, position: Vec2) {
    let mut factor = 0.0;
    let from = object.position;
    loop {
        factor = (factor + delta_time()).clamp(0.0, 1.0);
        object.position = from.lerp(position, factor);
        next_frame().await;
    }
}

spawn(async move {
    move_towards(game_object, target_position).await;
});
```

## Concurrent processing

> - `wait_for_any([<ASYNC WORK>])` (race).
> - `wait_for_all([<ASYNC WORK>])` (join).
> - Sometimes `select!`

Primitives like that tend to accept set of async work and "run" them at the same time, usually with expectation that all or some of them must end, in order to continue. Honestly, `select!` deserves an entire chapter on it's own, as it's rather a more rich but also can be more confusing primitive to work with, but this is not the right place to explain its inner workings.

```rust,no_run
async {
    // Character should reload gun while moving towards target.
    // If any of those end up quicker, we wait for all.
    wait_for_all([
        move_towards(game_object, target_position),
        reload_gun(),
    ]).await;
}
```

## Cancellation

> - `CancelToken`.
> - `is_cancelled(<TOKEN>)`.

Allows to define a way to tell some work from any point in a program, that we don't want to run some work anymore, and then that work, when it finds it's meaningful, can ask if it was instructed to be cancelled and stop immediatelly.

```rust,no_run
let token = CancelToken::default();
let token2 = token.clone();

spawn(async move {
    // Character moves towards target, but if player tells it to stop, it does
    // it immediatelly.
    wait_for_any([
        move_towards(game_object, target_position),
        token2,
    ]).await;
});

token.cancel();
```

## Direct polling points in game frame

While runtimes usually provide a single async work queue, that gets a single point in game frame, where we explicitly progress pending work, i've found that providing an ability to define work queues allows better cooperation with game systems ordering. Think: you might wanna do some work before physics step or after regular game objects update.

```rust,no_run
// Game frame loop.
loop {
    before_physics_queue.run();
    physics_system();
    game_object_update_system();
    after_update_queue.run();
}
```

```rust,no_run
// Tween game object position to position in time.
before_physics_queue.spawn(async move {
    move_towards(game_object, target_position).await;
});
```
