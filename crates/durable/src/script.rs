use intuicio_data::managed_gc::ManagedGc;
use moirai::{job::JobObject, jobs::JobsWaker};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_intermediate::{Intermediate, from_intermediate, to_intermediate};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

const SERIALIZATION: &str = "~moirai-serialization~";
const SERIALIZABLE_STACK: &str = "~moirai-serializable_stack~";
const TRANSIENT_STACK: &str = "~moirai-transient_stack~";
const LOCATION_STACK: &str = "~moirai-location_stack~";

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;
type BoxFutureFactory = Box<dyn FnMut() -> BoxFuture + Send + Sync>;
type Script = Vec<Operation>;

struct SerializationItem {
    type_name: &'static str,
    #[allow(clippy::type_complexity)]
    serialize: Box<dyn Fn(&dyn Any) -> Option<Intermediate> + Send + Sync>,
    #[allow(clippy::type_complexity)]
    deserialize: Box<dyn Fn(&Intermediate) -> Option<Box<dyn Any>> + Send + Sync>,
}

#[derive(Default)]
struct Serialization {
    table: HashMap<TypeId, SerializationItem>,
}

impl Serialization {
    pub fn register<T: Serialize + DeserializeOwned + 'static>(&mut self) {
        let type_id = TypeId::of::<T>();
        self.table.insert(
            type_id,
            SerializationItem {
                type_name: std::any::type_name::<T>(),
                serialize: Box::new(|data| {
                    let data = data.downcast_ref::<T>().unwrap();
                    to_intermediate(data).ok()
                }),
                deserialize: Box::new(|value| {
                    from_intermediate::<T>(value)
                        .ok()
                        .map(|data| Box::new(data) as Box<dyn Any>)
                }),
            },
        );
    }
}

#[derive(Default)]
struct DataStack {
    stack: Vec<Box<dyn Any>>,
}

impl DataStack {
    fn push(&mut self, value: impl Any) {
        self.stack.push(Box::new(value));
    }

    fn pop<T: Any>(&mut self) -> T {
        let object = self
            .stack
            .pop()
            .expect("DataStack: attempted to pop from an empty stack");
        match object.downcast::<T>() {
            Ok(boxed) => *boxed,
            Err(_) => panic!(
                "DataStack: type mismatch on data stack pop. Expected {}",
                std::any::type_name::<T>()
            ),
        }
    }

    fn pop_raw(&mut self) -> Box<dyn Any> {
        self.stack
            .pop()
            .expect("DataStack: attempted to pop from an empty stack")
    }

    fn serialize(&self, serialization: &Serialization) -> Option<DataStackSnapshot> {
        let mut values = Vec::with_capacity(self.stack.len());
        for item in &self.stack {
            let type_id = (**item).type_id();
            let serialization_item = serialization.table.get(&type_id)?;
            let intermediate = (serialization_item.serialize)(&**item)?;
            values.push(DataStackValue {
                type_name: serialization_item.type_name.to_string(),
                intermediate,
            });
        }
        Some(DataStackSnapshot(values))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DataStackValue {
    type_name: String,
    intermediate: Intermediate,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataStackSnapshot(Vec<DataStackValue>);

impl DataStackSnapshot {
    fn deserialize(&self, serialization: &Serialization) -> Option<DataStack> {
        let mut stack = Vec::with_capacity(self.0.len());
        for value in &self.0 {
            let serialization_item = serialization
                .table
                .values()
                .find(|item| item.type_name == value.type_name)?;
            let data = (serialization_item.deserialize)(&value.intermediate)?;
            stack.push(data);
        }
        Some(DataStack { stack })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Location {
    Scope { position: usize },
    Iterator { counter: usize, rehydrating: bool },
}

impl Location {
    fn position(&self) -> usize {
        match self {
            Location::Scope { position } => *position,
            Location::Iterator { .. } => {
                panic!("Iterator location does not have a position");
            }
        }
    }

    fn increment_position(&mut self) {
        match self {
            Location::Scope { position } => {
                *position += 1;
            }
            Location::Iterator { .. } => {
                panic!("Cannot increment position of Iterator location");
            }
        }
    }

    fn increment_counter(&mut self) {
        match self {
            Location::Scope { .. } => {
                panic!("Cannot increment counter of Scope location");
            }
            Location::Iterator { counter, .. } => {
                *counter += 1;
            }
        }
    }
}

enum Operation {
    Future {
        future: BoxFutureFactory,
    },
    Scope {
        body: Script,
    },
    Iterator {
        extract: Box<dyn FnMut(usize) + Send + Sync>,
        fetch: Box<dyn FnMut() -> bool + Send + Sync>,
        body: Script,
    },
}

impl Operation {
    // TODO: refactor this shit, it's unreadable and prone to error af.
    // especially iterator case, this thing keeps causing errors.
    fn poll_scope(
        scope: &mut [Self],
        level: usize,
        locations: &mut Vec<Location>,
        serializable_stack: &mut ManagedGc<DataStack>,
        curent_future: &mut Option<BoxFuture>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        if level >= locations.len() {
            return Poll::Ready(());
        }
        let position = locations[level].position();
        if position >= scope.len() {
            return Poll::Ready(());
        }
        match &mut scope[position] {
            Operation::Future { future } => {
                if curent_future.is_none() {
                    *curent_future = Some(future());
                }
                match curent_future.as_mut().unwrap().as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        locations[level].increment_position();
                        *curent_future = None;
                    }
                    Poll::Pending => {}
                }
            }
            Operation::Scope { body } => {
                if locations.len() <= level + 1 {
                    locations.push(Location::Scope { position: 0 });
                    serializable_stack.try_write().unwrap().push(());
                }
                match Self::poll_scope(
                    body,
                    level + 1,
                    locations,
                    serializable_stack,
                    curent_future,
                    cx,
                ) {
                    Poll::Ready(()) => {
                        locations[level].increment_position();
                        locations.pop();
                        serializable_stack.try_write().unwrap().pop_raw();
                    }
                    Poll::Pending => {}
                }
            }
            Operation::Iterator {
                extract,
                fetch,
                body,
            } => {
                if locations.len() <= level + 1 {
                    locations.push(Location::Iterator {
                        counter: 0,
                        rehydrating: false,
                    });
                    extract(0);
                } else if let Location::Iterator {
                    counter,
                    rehydrating,
                } = &mut locations[level + 1]
                    && *rehydrating
                {
                    extract(*counter);
                    *rehydrating = false;
                    if locations.len() <= level + 2 {
                        locations.push(Location::Scope { position: 0 });
                    }
                    fetch();
                }
                let execute = if locations.len() <= level + 2 {
                    locations.push(Location::Scope { position: 0 });
                    fetch()
                } else {
                    true
                };
                if execute {
                    match Operation::poll_scope(
                        body,
                        level + 2,
                        locations,
                        serializable_stack,
                        curent_future,
                        cx,
                    ) {
                        Poll::Ready(()) => {
                            locations[level + 1].increment_counter();
                            locations.pop();
                            serializable_stack.try_write().unwrap().pop_raw();
                        }
                        Poll::Pending => {}
                    }
                } else {
                    locations[level].increment_position();
                    locations.pop();
                    locations.pop();
                }
            }
        }
        if locations[level].position() >= scope.len() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub struct MoiraiScopeBuilder<T: Send + Serialize + DeserializeOwned + 'static> {
    serialization: Serialization,
    serializable_stack: ManagedGc<DataStack>,
    transient_stack: ManagedGc<DataStack>,
    scope: Script,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Send + Serialize + DeserializeOwned + 'static> MoiraiScopeBuilder<T> {
    fn new(
        serialization: Serialization,
        serializable_stack: ManagedGc<DataStack>,
        transient_stack: ManagedGc<DataStack>,
    ) -> Self {
        Self {
            serialization,
            serializable_stack,
            transient_stack,
            scope: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Serialize + DeserializeOwned + 'static> MoiraiScopeBuilder<T> {
    pub fn future<T2, G, F>(self, future: G) -> MoiraiScopeBuilder<T2>
    where
        T2: Send + Serialize + DeserializeOwned + 'static,
        F: Future<Output = T2> + Send + Sync + 'static,
        G: Fn(T) -> F + Send + Sync + 'static,
    {
        let Self {
            mut serialization,
            serializable_stack,
            transient_stack,
            mut scope,
            ..
        } = self;
        serialization.register::<T2>();
        let future = Arc::new(future);
        let sub_stack = serializable_stack.clone();
        scope.push(Operation::Future {
            future: Box::new(move || {
                let future = future.clone();
                let mut sub_stack = sub_stack.clone();
                Box::pin(async move {
                    let input = sub_stack.try_write().unwrap().pop::<T>();
                    let output = future(input).await;
                    sub_stack.try_write().unwrap().push(output);
                })
            }),
        });
        MoiraiScopeBuilder {
            serialization,
            serializable_stack,
            transient_stack,
            scope,
            _phantom: PhantomData,
        }
    }

    pub fn closure<T2, F>(self, future: F) -> MoiraiScopeBuilder<T2>
    where
        T2: Send + Serialize + DeserializeOwned + 'static,
        F: Fn(T) -> T2 + Send + Sync + 'static,
    {
        let Self {
            mut serialization,
            serializable_stack,
            transient_stack,
            mut scope,
            ..
        } = self;
        serialization.register::<T2>();
        let future = Arc::new(future);
        let sub_stack = serializable_stack.clone();
        scope.push(Operation::Future {
            future: Box::new(move || {
                let future = future.clone();
                let mut sub_stack = sub_stack.clone();
                Box::pin(async move {
                    let input = sub_stack.try_write().unwrap().pop::<T>();
                    let output = future(input);
                    sub_stack.try_write().unwrap().push(output);
                })
            }),
        });
        MoiraiScopeBuilder {
            serialization,
            serializable_stack,
            transient_stack,
            scope,
            _phantom: PhantomData,
        }
    }

    pub fn scope<T2, F>(self, body: F) -> Self
    where
        T2: Send + Serialize + DeserializeOwned + 'static,
        F: FnOnce(MoiraiScopeBuilder<()>) -> MoiraiScopeBuilder<T2>,
    {
        let Self {
            mut serialization,
            serializable_stack,
            transient_stack,
            mut scope,
            ..
        } = self;
        serialization.register::<T2>();
        let builder = body(MoiraiScopeBuilder::<()>::new(
            serialization,
            serializable_stack.clone(),
            transient_stack.clone(),
        ));
        scope.push(Operation::Scope {
            body: builder.scope,
        });
        MoiraiScopeBuilder {
            serialization: builder.serialization,
            serializable_stack,
            transient_stack,
            scope,
            _phantom: PhantomData,
        }
    }

    pub fn iter<I, FE, FB>(self, extract: FE, body: FB) -> Self
    where
        I: Iterator + 'static,
        I::Item: Send + Serialize + DeserializeOwned + 'static,
        FE: Fn(&T, usize) -> I + Send + Sync + 'static,
        FB: FnOnce(MoiraiScopeBuilder<I::Item>) -> MoiraiScopeBuilder<bool>,
    {
        let Self {
            mut serialization,
            serializable_stack,
            transient_stack,
            mut scope,
            ..
        } = self;
        serialization.register::<I::Item>();
        let builder = body(MoiraiScopeBuilder::<I::Item>::new(
            serialization,
            serializable_stack.clone(),
            transient_stack.clone(),
        ));
        let mut extract_serializable_stack = serializable_stack.clone();
        let mut extract_transient_stack = transient_stack.clone();
        let mut fetch_serializable_stack = serializable_stack.clone();
        let mut fetch_transient_stack = transient_stack.clone();
        scope.push(Operation::Iterator {
            extract: Box::new(move |counter| {
                let value = extract_serializable_stack.try_write().unwrap().pop::<T>();
                let iter = extract(&value, counter);
                extract_transient_stack.try_write().unwrap().push(iter);
                extract_serializable_stack.try_write().unwrap().push(value);
            }),
            fetch: Box::new(move || {
                let mut iter = fetch_transient_stack.try_write().unwrap().pop::<I>();
                let result = if let Some(item) = iter.next() {
                    fetch_serializable_stack.try_write().unwrap().push(item);
                    true
                } else {
                    false
                };
                fetch_transient_stack.try_write().unwrap().push(iter);
                result
            }),
            body: builder.scope,
        });
        MoiraiScopeBuilder {
            serialization: builder.serialization,
            serializable_stack,
            transient_stack,
            scope,
            _phantom: PhantomData,
        }
    }

    pub fn iter_mut<I, FE, FB>(self, extract: FE, body: FB) -> Self
    where
        I: Iterator + 'static,
        I::Item: Send + Serialize + DeserializeOwned + 'static,
        FE: Fn(&mut T, usize) -> I + Send + Sync + 'static,
        FB: FnOnce(MoiraiScopeBuilder<I::Item>) -> MoiraiScopeBuilder<bool>,
    {
        let Self {
            mut serialization,
            serializable_stack,
            transient_stack,
            mut scope,
            ..
        } = self;
        serialization.register::<I::Item>();
        let builder = body(MoiraiScopeBuilder::<I::Item>::new(
            serialization,
            serializable_stack.clone(),
            transient_stack.clone(),
        ));
        let mut extract_serializable_stack = serializable_stack.clone();
        let mut extract_transient_stack = transient_stack.clone();
        let mut fetch_serializable_stack = serializable_stack.clone();
        let mut fetch_transient_stack = transient_stack.clone();
        scope.push(Operation::Iterator {
            extract: Box::new(move |counter| {
                let mut value = extract_serializable_stack.try_write().unwrap().pop::<T>();
                let iter = extract(&mut value, counter);
                extract_transient_stack.try_write().unwrap().push(iter);
                extract_serializable_stack.try_write().unwrap().push(value);
            }),
            fetch: Box::new(move || {
                let mut iter = fetch_transient_stack.try_write().unwrap().pop::<I>();
                let result = if let Some(item) = iter.next() {
                    fetch_serializable_stack.try_write().unwrap().push(item);
                    true
                } else {
                    false
                };
                fetch_transient_stack.try_write().unwrap().push(iter);
                result
            }),
            body: builder.scope,
        });
        MoiraiScopeBuilder {
            serialization: builder.serialization,
            serializable_stack,
            transient_stack,
            scope,
            _phantom: PhantomData,
        }
    }
}

pub struct MoiraiScript<T: Send + Serialize + DeserializeOwned + 'static> {
    serialization: ManagedGc<Serialization>,
    serializable_stack: ManagedGc<DataStack>,
    transient_stack: ManagedGc<DataStack>,
    location_stack: ManagedGc<Vec<Location>>,
    scope: Script,
    current_future: Option<BoxFuture>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Send + Serialize + DeserializeOwned + 'static> MoiraiScript<T> {
    pub fn new(f: impl FnOnce(MoiraiScopeBuilder<()>) -> MoiraiScopeBuilder<T>) -> Self {
        let mut serialization = Serialization::default();
        serialization.register::<T>();
        let serializable_stack = ManagedGc::new(DataStack::default());
        let transient_stack = ManagedGc::new(DataStack::default());
        let builder = f(MoiraiScopeBuilder::<()>::new(
            serialization,
            serializable_stack.clone(),
            transient_stack.clone(),
        ));
        let serialization = builder.serialization;
        let scope = builder.scope;
        Self {
            serialization: ManagedGc::new(serialization),
            serializable_stack,
            transient_stack,
            location_stack: Default::default(),
            scope,
            current_future: None,
            _phantom: PhantomData,
        }
    }

    pub fn dehydrate(job: &JobObject) -> Option<MoiraiScriptSnapshot> {
        let serialization = job
            .meta()
            .get(SERIALIZATION)?
            .into_typed::<Serialization>()
            .ok()?;
        let serializable_stack = job
            .meta()
            .get(SERIALIZABLE_STACK)?
            .into_typed::<DataStack>()
            .ok()?;
        let location_stack = job
            .meta()
            .get(LOCATION_STACK)?
            .into_typed::<Vec<Location>>()
            .ok()?;
        // TODO: miri throws stacked borrows errors pointing here. need to fix
        // ManagedGc lazy access, as retag doesn't see created references.
        let serializable_stack_snapshot = serializable_stack
            .read()
            .unwrap()
            .serialize(&serialization.read().unwrap())
            .expect("Failed to serialize serializable stack");
        let location_stack = location_stack
            .read()
            .unwrap()
            .iter()
            .map(|location| match *location {
                Location::Iterator { counter, .. } => Location::Iterator {
                    counter,
                    rehydrating: true,
                },
                value => value,
            })
            .collect();
        Some(MoiraiScriptSnapshot {
            serializable_stack: serializable_stack_snapshot,
            location_stack,
        })
    }

    #[allow(clippy::result_large_err)]
    pub fn rehydrate(mut self, snapshot: MoiraiScriptSnapshot) -> Result<Self, Self> {
        let Some(serializable_stack) = snapshot
            .serializable_stack
            .deserialize(&self.serialization.try_read().unwrap())
        else {
            return Err(self);
        };
        *self.serializable_stack.try_write().unwrap() = serializable_stack;
        *self.location_stack.try_write().unwrap() = snapshot.location_stack;
        Ok(self)
    }
}

impl<T: Send + Serialize + DeserializeOwned + 'static> Future for MoiraiScript<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let location_stack_lazy = this.location_stack.lazy();
        let Some(mut location_stack) = this.location_stack.try_write() else {
            return Poll::Pending;
        };
        if location_stack.is_empty() {
            location_stack.push(Location::Scope { position: 0 });
            this.serializable_stack.try_write().unwrap().push(());
            if let Some(waker) = JobsWaker::try_cast(cx.waker()) {
                let runtime = unsafe { waker.runtime() };
                runtime
                    .local_meta
                    .set(SERIALIZATION, this.serialization.lazy().into_dynamic());
                // TODO: this violates stacked borrows rules, need to fix ManagedGc lazy access.
                // It happen only at some specific points during dehydration.
                runtime.local_meta.set(
                    SERIALIZABLE_STACK,
                    this.serializable_stack.lazy().into_dynamic(),
                );
                runtime
                    .local_meta
                    .set(TRANSIENT_STACK, this.transient_stack.lazy().into_dynamic());
                runtime
                    .local_meta
                    .set(LOCATION_STACK, location_stack_lazy.into_dynamic());
            }
        }
        match Operation::poll_scope(
            &mut this.scope,
            0,
            &mut location_stack,
            &mut this.serializable_stack,
            &mut this.current_future,
            cx,
        ) {
            Poll::Ready(()) => {
                location_stack.pop();
                assert!(location_stack.is_empty());
                this.transient_stack.try_write().unwrap().pop_raw();
                assert!(this.transient_stack.try_read().unwrap().stack.is_empty());
                assert_eq!(this.serializable_stack.try_read().unwrap().stack.len(), 1);
                Poll::Ready(this.serializable_stack.try_write().unwrap().pop::<T>())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MoiraiScriptSnapshot {
    serializable_stack: DataStackSnapshot,
    location_stack: Vec<Location>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use moirai::{
        coroutine::wait_polls,
        job::{JobLocation, JobOptions},
        jobs::Jobs,
    };
    use std::time::Duration;

    struct Durable;

    #[test]
    fn test_durable_snapshot() {
        fn make_future() -> MoiraiScript<String> {
            MoiraiScript::new(|scope| {
                scope
                    .future(async |_| {
                        println!("Yield 42");
                        wait_polls(1).await;
                        42
                    })
                    .future(async |value| {
                        println!("Add 1: {}", value);
                        wait_polls(1).await;
                        value + 1
                    })
                    .scope(|scope| {
                        scope.future(async |_| 0..10).iter(
                            |range, skip| range.clone().skip(skip),
                            |scope| {
                                scope.future(async |index| {
                                    println!("Index: {}", index);
                                    wait_polls(1).await;
                                    true
                                })
                            },
                        )
                    })
                    .future(async |value| {
                        println!("Multiply by 2: {}", value);
                        wait_polls(1).await;
                        value * 2
                    })
                    .closure(|value| value.to_string())
            })
        }

        let future = make_future();
        let jobs = Jobs::new(0, Duration::MAX);
        jobs.spawn(
            JobOptions::default()
                .location(JobLocation::Local)
                .tag::<Durable>(),
            future,
        );
        for _ in 0..10 {
            jobs.run_local();
        }

        let job_object = jobs
            .queue()
            .extract_if(|job| job.tags().contains::<Durable>())
            .pop()
            .unwrap();
        let snapshot = MoiraiScript::<String>::dehydrate(&job_object).unwrap();
        println!("* Dehydrating future into snapshot");

        println!("* Rehydrating future from snapshot");
        let future = make_future().rehydrate(snapshot).ok().unwrap();
        jobs.block_on(future);
    }
}
