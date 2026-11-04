use super::error::LinkExpressionError;
use geoengine_expression::{
    ExpressionAst, ExpressionDependencies, LinkedExpression, error::ExpressionExecutionError,
};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Once, PoisonError},
    time::{Duration, Instant},
};
use tokio::sync::{OnceCell, Semaphore};

/// Limits for keeping compiled expressions.
/// The default values are the same as in `Settings-default.toml`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpressionCacheConfig {
    /// How long an unused library stays loaded for its next user
    pub idle_ttl: Duration,
    /// How often unused libraries are searched for
    pub sweep_interval: Duration,
    /// How many unused libraries are kept
    pub max_idle_libraries: usize,
    /// How many expressions are compiled at the same time
    pub max_concurrent_compilations: NonZeroUsize,
}

impl Default for ExpressionCacheConfig {
    fn default() -> Self {
        Self {
            idle_ttl: Duration::from_mins(15),
            sweep_interval: Duration::from_secs(30),
            max_idle_libraries: 256,
            max_concurrent_compilations: NonZeroUsize::new(8).expect("8 is not zero"),
        }
    }
}

type LibrarySlot = Arc<OnceCell<Arc<LinkedExpression>>>;

struct Library {
    slot: LibrarySlot,
    last_used: Instant,
}

impl Library {
    /// New references only come from the map, whose lock the caller holds.
    /// Thus, an unused library stays unused until the lock is released.
    fn is_in_use(&self) -> bool {
        Arc::strong_count(&self.slot) > 1
            || self
                .slot
                .get()
                .is_some_and(|library| Arc::strong_count(library) > 1)
    }
}

struct Inner {
    config: ExpressionCacheConfig,
    dependencies: OnceCell<Arc<ExpressionDependencies>>,
    /// The key is the generated code. It is not hashed, since a collision would run the wrong expression.
    /// It contains the name and signature of the function, so the libraries of equal keys are interchangeable.
    libraries: Mutex<HashMap<String, Library>>,
    sweeper: Once,
    compilations: Arc<Semaphore>,
}

impl Inner {
    async fn dependencies(&self) -> Result<&Arc<ExpressionDependencies>, LinkExpressionError> {
        self.dependencies
            .get_or_try_init(|| async {
                let dependencies =
                    crate::util::spawn_blocking(ExpressionDependencies::new).await??;
                Ok::<_, LinkExpressionError>(Arc::new(dependencies))
            })
            .await
    }

    fn sweep(&self) {
        let removed = {
            let mut libraries = self
                .libraries
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            remove_unused(&mut libraries, Instant::now(), &self.config)
        };

        // unloading and deleting is slow, so it must not happen while holding the lock
        drop(removed);
    }

    /// New libraries can arrive much faster than the regular sweeps.
    fn sweep_if_above_limit(&self) {
        let is_above_limit = self
            .libraries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
            > self.config.max_idle_libraries;

        if is_above_limit {
            self.sweep();
        }
    }
}

/// Compiles expressions and shares the resulting libraries between all of their users.
/// Unused libraries are unloaded and deleted according to the [`ExpressionCacheConfig`].
///
/// Create one `ExpressionEnvironment` per application and clone it into every execution context.
/// Creating it lazily avoids compiling the dependencies if no expression is used.
#[derive(Clone)]
pub struct ExpressionEnvironment(Arc<Inner>);

impl ExpressionEnvironment {
    pub fn new(config: ExpressionCacheConfig) -> Self {
        Self(Arc::new(Inner {
            config,
            dependencies: OnceCell::new(),
            libraries: Mutex::default(),
            sweeper: Once::new(),
            compilations: Arc::new(Semaphore::new(config.max_concurrent_compilations.get())),
        }))
    }

    /// Compiles the dependencies so that they can be reused for all expression evaluations.
    /// Compiling the dependencies takes a while, so this can drastically improve performance on the first expression call.
    ///
    /// If it fails, you can retry or terminate the program.
    ///
    pub async fn initialize(&self) -> Result<(), LinkExpressionError> {
        self.0.dependencies().await?;
        Ok(())
    }

    /// Returns the library of the expression and compiles it if nobody did so before.
    /// Concurrent callers wait for the same compilation.
    ///
    /// # Panics
    ///
    /// Never, the semaphore limiting the compilations is not closed.
    ///
    pub async fn link(
        &self,
        ast: &ExpressionAst,
    ) -> Result<Arc<LinkedExpression>, LinkExpressionError> {
        self.0.sweeper.call_once(|| spawn_sweeper(&self.0));

        let code = ast.code();

        let slot = {
            let mut libraries = self
                .0
                .libraries
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let library = libraries.entry(code.clone()).or_insert_with(|| Library {
                slot: LibrarySlot::default(),
                last_used: Instant::now(),
            });
            library.last_used = Instant::now();
            library.slot.clone()
        };

        let library = slot
            .get_or_try_init(|| async {
                let dependencies = self.0.dependencies().await?.clone();
                let name = ast.name().to_owned();
                let inner = self.0.clone();
                let permit = self
                    .0
                    .compilations
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("the semaphore is never closed");
                let library = crate::util::spawn_blocking(move || {
                    // held until the compiler is done, even if the request gets cancelled
                    let _permit = permit;
                    let library = LinkedExpression::new(&name, &code, &dependencies)?;
                    inner.sweep_if_above_limit();
                    Ok::<_, ExpressionExecutionError>(library)
                })
                .await??;
                Ok::<_, LinkExpressionError>(Arc::new(library))
            })
            .await?;

        Ok(library.clone())
    }
}

/// The thread only holds a weak reference and ends together with the environment.
fn spawn_sweeper(inner: &Arc<Inner>) {
    let sweep_interval = inner.config.sweep_interval;
    let inner = Arc::downgrade(inner);

    let sweeper = std::thread::Builder::new()
        .name("expression-sweeper".to_owned())
        .spawn(move || {
            loop {
                std::thread::sleep(sweep_interval);

                let Some(inner) = inner.upgrade() else {
                    return;
                };
                inner.sweep();
            }
        });

    if let Err(e) = sweeper {
        tracing::error!("Cannot start the cleanup of unused expressions: {e}");
    }
}

/// Takes the libraries out of the map that should be unloaded.
fn remove_unused(
    libraries: &mut HashMap<String, Library>,
    now: Instant,
    config: &ExpressionCacheConfig,
) -> Vec<Library> {
    // a query can run longer than the TTL, so the idle time starts at the last sweep that saw it in use
    for library in libraries.values_mut() {
        if library.is_in_use() {
            library.last_used = now;
        }
    }

    let mut removed: Vec<Library> = libraries
        .extract_if(|_, library| {
            !library.is_in_use() && now.duration_since(library.last_used) >= config.idle_ttl
        })
        .map(|(_, library)| library)
        .collect();

    let mut last_used_of_idle: Vec<Instant> = libraries
        .values()
        .filter(|library| !library.is_in_use())
        .map(|library| library.last_used)
        .collect();

    let number_of_excess_libraries = last_used_of_idle
        .len()
        .saturating_sub(config.max_idle_libraries);

    if number_of_excess_libraries > 0 {
        // libraries with the same timestamp are removed together, which is harmless
        let (_, &mut newest_to_remove, _) =
            last_used_of_idle.select_nth_unstable(number_of_excess_libraries - 1);

        removed.extend(
            libraries
                .extract_if(|_, library| {
                    !library.is_in_use() && library.last_used <= newest_to_remove
                })
                .map(|(_, library)| library),
        );
    }

    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test::test_expression_environment;
    use geoengine_expression::{DataType, ExpressionParser, Parameter};

    const TTL: Duration = Duration::from_secs(100);

    fn config(max_idle_libraries: usize) -> ExpressionCacheConfig {
        ExpressionCacheConfig {
            idle_ttl: TTL,
            sweep_interval: Duration::from_secs(1),
            max_idle_libraries,
            max_concurrent_compilations: NonZeroUsize::MIN,
        }
    }

    fn parse(expression: &str) -> ExpressionAst {
        ExpressionParser::new(&[Parameter::Number("A".into())], DataType::Number)
            .unwrap()
            .parse("expression", expression)
            .unwrap()
    }

    fn library(last_used: Instant) -> Library {
        Library {
            slot: LibrarySlot::default(),
            last_used,
        }
    }

    #[test]
    fn it_removes_unused_libraries_after_the_ttl() {
        let start = Instant::now();
        let mut libraries = HashMap::from([("a".to_owned(), library(start))]);

        let removed = remove_unused(&mut libraries, start + TTL / 2, &config(10));
        assert!(removed.is_empty());

        let removed = remove_unused(&mut libraries, start + TTL, &config(10));
        assert_eq!(removed.len(), 1);
        assert!(libraries.is_empty());
    }

    #[test]
    fn it_keeps_libraries_in_use_and_starts_the_ttl_after_their_use() {
        let start = Instant::now();
        let mut libraries = HashMap::from([("a".to_owned(), library(start))]);
        let user = libraries["a"].slot.clone();

        let last_seen_in_use = start + TTL * 10;
        let removed = remove_unused(&mut libraries, last_seen_in_use, &config(0));
        assert!(removed.is_empty());

        drop(user);

        let removed = remove_unused(&mut libraries, last_seen_in_use + TTL / 2, &config(10));
        assert!(removed.is_empty());

        let removed = remove_unused(&mut libraries, last_seen_in_use + TTL, &config(10));
        assert_eq!(removed.len(), 1);
    }

    #[test]
    fn it_removes_the_oldest_unused_libraries_above_the_limit() {
        let start = Instant::now();
        let now = start + Duration::from_secs(3);
        let mut libraries = HashMap::from([
            ("in_use".to_owned(), library(start)),
            ("oldest".to_owned(), library(start)),
            ("older".to_owned(), library(start + Duration::from_secs(1))),
            ("newest".to_owned(), library(start + Duration::from_secs(2))),
        ]);
        let _user = libraries["in_use"].slot.clone();

        let removed = remove_unused(&mut libraries, now, &config(1));

        assert_eq!(removed.len(), 2);
        assert!(libraries.contains_key("in_use"));
        assert!(libraries.contains_key("newest"));
    }

    #[tokio::test]
    async fn it_shares_the_library_of_equal_expressions() {
        let environment = test_expression_environment();
        let ast = parse("A + 1");

        let (first, second) = tokio::join!(environment.link(&ast), environment.link(&ast));
        let (first, second) = (first.unwrap(), second.unwrap());
        assert!(Arc::ptr_eq(&first, &second));

        let third = environment.link(&ast).await.unwrap();
        assert!(Arc::ptr_eq(&first, &third));
    }

    #[tokio::test]
    async fn it_unloads_unused_libraries_with_the_sweep_thread() {
        let environment = ExpressionEnvironment::new(ExpressionCacheConfig {
            idle_ttl: Duration::ZERO,
            sweep_interval: Duration::from_millis(10),
            ..ExpressionCacheConfig::default()
        });

        let library = environment.link(&parse("A + 1")).await.unwrap();
        let unloaded = Arc::downgrade(&library);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(unloaded.upgrade().is_some(), "library is still in use");

        drop(library);

        tokio::time::timeout(Duration::from_secs(10), async {
            while unloaded.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the sweep thread should unload the library");
    }

    #[tokio::test]
    async fn it_enforces_the_limit_without_waiting_for_the_sweep_thread() {
        let environment = ExpressionEnvironment::new(ExpressionCacheConfig {
            idle_ttl: Duration::from_hours(1),
            sweep_interval: Duration::from_hours(1),
            ..config(1)
        });

        let first = environment.link(&parse("A + 1")).await.unwrap();
        let unloaded = Arc::downgrade(&first);
        drop(first);
        drop(environment.link(&parse("A + 2")).await.unwrap());
        assert!(unloaded.upgrade().is_some(), "one unused library may stay");

        // a single compilation permit must not block concurrent callers forever
        let (third, fourth) = (parse("A + 3"), parse("A + 4"));
        let (third, fourth) = tokio::join!(environment.link(&third), environment.link(&fourth));
        third.unwrap();
        fourth.unwrap();

        assert!(unloaded.upgrade().is_none());
    }
}
