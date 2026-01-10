# UI dialogue

In this chapter we will show event-driven logic based on UI dialogue system in simple text-based game, where dialogue widget shows a message and awaits for user to decide on the option to choose from.

Pattern used in this example applies not only to text-based games, but also in any modern game that has dialogues for conversations - similar approach even applies cut scenes system. The point here is to showcase suspension and resumes with events.

## UI dialogue system with events

> Typical dialogue system uses asset-driven container type for storing conversation points that have message with available options, that each points to another conversation point when confirmed, effectively building a graph of conversation flow.
>
> Asset-driven systems like that are rightfully state machines and it's nothing wrong with that per se, but i want to make sure you, dear reader, understand that we are not talking about dialogue system specifically, rather we talk about event-driven logic in general, so please don't dismiss this example, as it's goal is to demonstrate events orchestrating suspension, not focusing on a game feature.

Let's start with a widget object:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_events.rs:widget}}
```

Usually dialogue widget has some information about what conversation point we are showing with what options, as well as some means to notify game about user taking decision - here we use events to signal confirming user-selected option.

Within this widget we also react on user changing selected option, confirming which sends an event to game:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_events.rs:confirm-selection}}
```

Then somewhere there is a game system, that listens for such events and handle updating widget to point to new conversation point:

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_events.rs:receive-events}}
```

<details>
<summary><code>Finally, let's show the conversation graph we've made, just to show you some scale:</code></summary>

```rust,no_run
{{#rustdoc_include ../../../samples/examples/traditional_events.rs:conversation}}
```

</details>

## Observations

This pattern of event-driven asynchronous logic flow is correct and widely used in games, because it allows to properly await for game state changes, and it's easy to implement.

### **_Clean declaration, messy execution_**

When we compare declararing conversation graph to how it's executed, we can see the difference - IMHO the declarative part is the only part that keeps it sane to reason about, as it tells the user in single place, how high level flow in the graph looks like.

But now imagine scenarios where you can't or don't express your asynchronous logic that way, instead you just have game systems and objects coupled with events sent between them - suddenly the cognitive load required to follow through the logic becomes challenging, the bigger the scale of game systems and objects interactions, more events sent from unrelated places to objects and systems that will handle all events important to them in a somewhat single place - it gets really messy real quick! It's really hard to maintain the bigger it gets.

I'm sure you have experienced such event-driven code and maybe even hate the complexity at some point.

### **_Hard to enforce correct and stable flow_**

At the moment we have very simple flow loop:

> ... Show dialogue -> Wait for user confirmation ...

Let's say we want to add other dialogue feature such as automatic progression after specified time, blocking input until voice-over is done. In that case we grow in number of states, we need to rework entive dialogue system to incorporate that.

We no longer rely only on conversation points but also on various other states dialogue can be in, effectively turning it into _real state machine_, making it harder to ensure some received events will only be able to get executed if some properties are in very specific state, like not reacting to show dialogue event if widget is blocked for some time, or to invalidate auto-progressing timer, when user receives user confirmation before.

Similarily, thinking about general event-driven logic, i bet you've had experienced or seen cases, where some game systems reacted on some events when they weren't expecting it to happen, invalidating current state.

For example: your character is in the middle of a cutscene, while game enviroment system spawned a fire next to you, spreading during cutscene, killing the player before cutscene ends - not fun experience, and usually hacked with turning player invincible for duration of cutscene, or generally requiring to carefuly disabling some game systems just to not make it happen.
