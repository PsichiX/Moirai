# Status effects

In previous chapter we have shown a battle loop and player turn - in this chapter we will take a look at one future we call in there: casting spells.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:fire-spell}}
```

If we have enough mana for given spell cost, we spawn new job that will run in the background.

> Spawning jobs enables concurrency - an **ability to run multiple jobs at the same time**.
>
> If you come from Unity (coroutines) or JavaScript (timers/async-await), or you just used threads before to achieve background work, you might have been using similar pattern already!

Spawned fire spell itself does nothing at the time of casting it (except deducing mana), but the spawned fire effect job will do its work each turn.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/moirai_jrpg_battle.rs:fire-spell-effect}}
```

Quite simpler to understand than explicit state machines signalled with events, don't you think? Maintanance and expanding of such logic is also quite easy to perform, as there isn't much of moving parts scattered around that need careful focus to connect the dots.
