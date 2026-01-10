# Turn-based JRPG walkthrough

In this section we will build a small turn-based JRPG with player and enemies, slashing eachother with swords, casting spells, giving multi-turn status effects.

This is not gonna be a full game with lots of game systems - it's gonna be specifically simplified to the point of showing only how to marry traditional sync game systems with async coroutines where it makes sense, without much focus on perfect game mechanics and superb game design - using Moirai library.

None of this is hard to do alone in traditional state machine pattern, but they become hard when they overlap and communicate - we will try to show how coroutines makes them easier to deal with and especially easier to reason about!
