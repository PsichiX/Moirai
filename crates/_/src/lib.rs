pub mod coroutine;
pub mod generator;
pub mod job;
pub mod jobs;
pub mod promise;
pub mod queue;

#[inline]
pub(crate) fn traced_spin_loop() {
    #[cfg(feature = "deadlock-trace")]
    println!(
        "* DEADLOCK BACKTRACE: {}",
        std::backtrace::Backtrace::force_capture()
    );
    std::hint::spin_loop();
}

pub mod third_party {
    pub use intuicio_data;
    pub use tracing;

    pub mod time {
        #[cfg(target_arch = "wasm32")]
        pub use instant::{Duration, Instant};
        #[cfg(not(target_arch = "wasm32"))]
        pub use std::time::{Duration, Instant};
    }
}
