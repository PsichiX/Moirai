# Introduction

This book is mainly targetted towards game developers, ones that are familiar with basics of Rust, ones that are familiar with async/await but only used it for IO, ones that came from other languages or game engines and are used to using coroutines for game logic in there and are looking for how to achieve such concept in Rust.

And additionally, this book is targetted also towards beginners, as it attempts to explain in simpler, non-formal way, how async works, using more real life than abstract scenarios.

This book most likely is not for advanced future-wizards, as it might feel oversimplifying entire concepts that you might find _important to be explained the proper way_ - don't worry, there are other, better and more formal materials for advanced async/await topics. But if you're still curious about game developer point of view, i'll be happy to share it with you too!

## What we are used to

While working on games, we tend to express game logic in form of an actions timeline deferred in time, awaiting for some game changes to react on and change to another state.

> The simplest scenario is non-blocking loading screen, where before we jump into the game session, we request set of assets to load, wait for it's progress to end, tracking it for loading bar visuals, and after all assets load, we actually change to game session state.

Usually we just trigger non-blocking asset loading request, on update we check if assets are loaded and only then we change state. We could express all of that as awaitable set of actions in a couple of lines of code. In gamedev we call this concept a **coroutine** - an asynchronous, suspendable code block or function, that awaits for some other work or change to complete, in order to continue it's own work.

The typical ways game developers handle such states is with enum variants and flags read and write in various places all over the code, or a bit smarter with dedicated state machine objects (but still controlled from various external places).

I'm not saying it's necessarily a bad way to handle suspendable logic - of course it works and therefore people use this pattern. But some patterns i've seen in games, could benefit from coroutines instead of manually driven state machines, IMHO.

## What we could do instead

While researching the topic, i see a general lack of (or very limited support of) such concept in Rust's Game Development area. I've had a pleasure working with coroutines in other languages in various game engines and i've found that lack of support dedicated towards games is quite astonishing to me, but i understand why it might be the case.

If you think **Async in Rust**, you immediatelly think **IO**, and i don't blame you - it was somewhat made for it mainly if not exclusively, but during my experiments with coroutines in Rust, i've found that one can express suspendable game logic in surprisingly ergonomic way with coroutines, compared to state machine spaghetti all over the game code!

> Less cognitive load means more pleasant and fun times making your games :)

## What we will learn

In this book i'm gonna try to show the idea of coroutines in Rust for gamedevs that came to Rust from other languages and engines supporting coroutine game logic, as well as for those who think async equals IO - that it's capable of more than IO and how to utilize this pattern.

We will cover:

- Coroutines as action timelines.
- User-side asynchronous suspension instead of per-frame polling.
- Fearless coroutines as means for cleaner, more readable and easier to maintain and refactor game logic.
- Local vs background job modes and means to move between those and change priorities.
- Suspendable game logic that survives saves.
