# Coroutine orchestration patterns

In this chapter we will be showing you patterns widely used to manage coroutines.

Grand rule for good and useful coroutine patterns:

> Every coroutine must have a clear owner, lifetime and exit condition.

## Sequential

You are already familiar with this pattern - we just run coroutines sequentially. The basic and most used way to run your work.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:sequential}}
```

## Fire and forget

Spawn some job in the background to run concurently alongside. Be careful, as if this job will end only when it decides, as it is detached from any other job, it effectively has no owner 0 it's completely independent job.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:fire-and-forget}}
```

> Be careful when spawning long-lived jobs as they will run for entire runtime lifetime!

## Parent-child

We spawn some job, but we also await on its result at some point, so currently running job is its child job owner, as it's a dependent job.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:parent-child}}
```

## Wait for all (join)

Spawning series of jobs that lets you do parallel work, with expectation to get results of them all when all are done.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:wait-for-all}}
```

## Wait for any (race)

Spawning series of jobs for parallel work, when you only care about result of the fastest job to complete.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:wait-for-any}}
```

## Wait for some

Spawning series of jobs with clear expectations which job results are required and which can be ignored when all required jobs complete - it's a mix of any and all.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:wait-for-some}}
```

## Supervisor

Spawning child jobs which lifetime are controlled by another job (supervisor). Usually used to restart jobs that failed their completion for some reason.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:supervisor}}
```

## Actor

Actors are jobs that do isolated work that communicates with outside world only via message passing.

```rust,no_run
{{#rustdoc_include ../../samples/examples/moirai_patterns.rs:actor}}
```
