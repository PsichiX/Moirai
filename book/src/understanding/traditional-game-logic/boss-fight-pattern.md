# Boss fight pattern

I've choosen to show simplified version, because the actual boss fight state machines typically look quite more complex as the state machines are rather much bigger in actual games, and we don't want to overwhelm ourselves with such complexity - this version will still remind you of something you might know from your own or others experience.

The game mechanics of simplified version are:

- Character (player or NPC) accumulates stamina.
- Stamina can be spent on either attacking or blocking:
  - Attack takes accumulated stamina and deals its value as damage to the opponent.
  - Block takes accumulated stamina and makes opponent unable to damage us for the duration of accumulated stamina.
- While in blocking state, character cannot perform any other action until cooldown drops to zero - no attacking, no blocking, no stamina regeneration.

## Boss fight pattern with state machines

What we have is usually NPC state variants that tell what actions NPC will try to perform:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_state_machines.rs:fight-state}}
```

And our boss fight pattern then composes a list of actions too build a pattern for its fighting style:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_state_machines.rs:fight-pattern}}
```

> Let me remind you, that for the sake of simplicity, we assume this is entire AI state machine, but in real games this might be just a single branch of a behavior tree - you get the idea, i hope!

Then in enemy update system we execute logic of an active state:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_state_machines.rs:update-enemy-system}}
```

<details>
<summary><code>Update Player System</code></summary>

We omit talking about player update system, as it doesn't use any fight patterns and it's mostly about reacting to user input, but as you can see, both player and NPC only execute actions of the character.

We don't make the mistake of state machine doing what actions do - state machine is only orchestrating characters to execute said actions in proper time.

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_state_machines.rs:update-player-system}}
```

> In game development we tend to try not to repeat ourselves more than we need to, so player and NPC are only different in how we control the characters, therefore they share actions execution logic, not needing to pollute each of their update systems with direct manipulation of common character state.

</details>

## Observations

First and foremost: this logic pattern isn't bad or wrong, it does its job and it does it well. That's why game developers use it. Nothing wrong with that!

### **_This pattern encodes a timeline of actions using data with control flow_**

It's not actually a state machine in spirit, it's a script of actions that step in reaction to state data changes.

`charge -> attack -> charge -> block -> charge -> attack`

The expressed intent is sequential, but the execution model is reactive. The mismatch is the problem, as you are expected to follow this _script_ by jumping back and forth between actions list and the code they evaluate into.

### **_Timing is implicit and scattered_**

Where does time live?

- stamina regeneration - `Character::update()`.
- blocking duration - hidden inside `Character::block_opponent()` action.
- charge duration - inferred via stamina threshold.
- pattern looping - index switching.

Time is not represented directly, it emerges from side effects across systems. Tuning gets hard, debugging harder and extending behavior risky.

### **_Progression is manual and fragile_**

This line: `enemy.fight_phase_index += 1;` is effectively a _program counter_. This logic handler behaves like a mini-virtual-machine. In order to switch state, one has to increment program counter and if it doesn't happen in right place at the right time, character gets stuck in state because we are expressing sequential progression with manual state changes.

Although AI systems behind decision making primitives tend to do manual state changes hidden from user eyes, so let's agree it's not the pattern problem per se, as user don't need to do it manually if uses decision making primitives. That's fine.

### **_The logic is frame-driven, not action-driven_**

The NPC doesn't _charge until stamina is ready_ - it checks every frame whether charging is done. Intent expressed in reactivity. It gives feeling of logic spread in time, while execution is fragmented.

Take a look at the `NPC_FIGHT_PATTERN` and `update_player_system` - notice how the npc fight pattern is readable, but the execution is not. The behavior is sequential, but code executing it isn't.

### **_Complexity scales non-linearly_**

The more this AI code evolve, the more states in it, the less code becomes readable without switching between sequential and reactive mindset more frequently, while trying to decypher the behavior. This solution doesn't scale well, because it requires more cognitive load the bigger it gets.

Every new requirement multiplies state interactions, not lines of code. It gets really tricky at that point, trying to follow the behavior. The code is correct, yet hostile to changes i would say (i would urge you, dear reader, to imagine 20 states instead of 3 - you might start to scratch your head).

**Refactors might feel scary at that point**.
