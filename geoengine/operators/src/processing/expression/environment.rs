use super::error::LinkExpressionError;
use geoengine_expression::{ExpressionAst, ExpressionDependencies, LinkedExpression};
use moka::future::Cache;
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, Once},
    time::Duration,
};
use tokio::sync::{OnceCell, Semaphore};

/// Limits for keeping compiled expressions, see `[expression]` in `Settings-default.toml`.
#[derive(Clone, Debug)]
pub struct ExpressionCacheConfig {
    /// How long a library stays in the cache without being requested
    pub idle_ttl: Duration,
    /// How often expired libraries are removed, also if nobody uses the cache
    pub sweep_interval: Duration,
    /// How many bytes of libraries the cache holds
    pub max_cache_size_in_bytes: u64,
    /// How many expressions are compiled at the same time
    pub max_concurrent_compilations: NonZeroUsize,
    /// The directory that holds the workspaces of all processes using it
    pub workspace_path: PathBuf,
}

struct Inner {
    /// Maps generated code to its compiled and loaded shared library (`.so`).
    /// The key is not hashed, since a collision would run the wrong expression.
    /// It contains the name and signature of the function, so the libraries of equal keys are interchangeable.
    ///
    /// A removed library stays loaded until its last user drops it.
    libraries: Cache<Arc<str>, Arc<LinkedExpression>>,
    dependencies: OnceCell<Arc<ExpressionDependencies>>,
    maintenance: Once,
    compilations: Arc<Semaphore>,
    sweep_interval: Duration,
    workspace_path: PathBuf,
}

impl Inner {
    /// Returns the cargo workspace with the pre-built dependencies, which every compilation links against.
    /// It is built by the first caller. A failed build is not stored, so the next caller tries again.
    async fn dependencies(&self) -> Result<&Arc<ExpressionDependencies>, LinkExpressionError> {
        self.dependencies
            .get_or_try_init(|| async {
                let workspace_path = self.workspace_path.clone();
                let dependencies = crate::util::spawn_blocking(move || {
                    ExpressionDependencies::new(&workspace_path)
                })
                .await??;
                Ok::<_, LinkExpressionError>(Arc::new(dependencies))
            })
            .await
    }
}

/// Compiles expressions and shares the resulting libraries between all of their users.
/// Libraries are unloaded and deleted according to the [`ExpressionCacheConfig`].
///
/// Create one `ExpressionEnvironment` per application and clone it into every execution context.
/// Creating it lazily avoids compiling the dependencies if no expression is used.
#[derive(Clone)]
pub struct ExpressionEnvironment(Arc<Inner>);

impl ExpressionEnvironment {
    pub fn new(config: ExpressionCacheConfig) -> Self {
        let libraries = Cache::builder()
            .max_capacity(config.max_cache_size_in_bytes)
            .time_to_idle(config.idle_ttl)
            .weigher(|_code: &Arc<str>, library: &Arc<LinkedExpression>| {
                u32::try_from(library.size_in_bytes()).unwrap_or(u32::MAX)
            })
            // unloading and deleting is slow, so it must not block an async worker;
            // a library still in use is unloaded wherever its last user drops it
            .eviction_listener(|_code, library, _cause| {
                crate::util::spawn_blocking(move || drop(library));
            })
            .build();

        Self(Arc::new(Inner {
            libraries,
            dependencies: OnceCell::new(),
            maintenance: Once::new(),
            compilations: Arc::new(Semaphore::new(config.max_concurrent_compilations.get())),
            sweep_interval: config.sweep_interval,
            workspace_path: config.workspace_path,
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

    /// Returns the library of the expression and compiles it if the cache does not hold it.
    /// Concurrent callers wait for the same compilation.
    pub async fn link(
        &self,
        ast: ExpressionAst,
    ) -> Result<Arc<LinkedExpression>, Arc<LinkExpressionError>> {
        self.0.maintenance.call_once(|| spawn_maintenance(&self.0));

        // shared by the key and the compilation
        let code: Arc<str> = ast.code().into();

        self.0
            .libraries
            .try_get_with(code.clone(), async {
                let dependencies = self.0.dependencies().await?.clone();
                let permit = self.0.compilations.clone().acquire_owned().await?;
                let library = crate::util::spawn_blocking(move || {
                    // held until the compiler is done, even if the request gets cancelled
                    let _permit = permit;
                    LinkedExpression::new(ast.name(), &code, &dependencies)
                })
                .await??;
                Ok::<_, LinkExpressionError>(Arc::new(library))
            })
            .await
    }
}

/// Starts the task that removes expired libraries.
/// The cache only does this while it is used, but an idle server should clean up as well.
/// The task only holds a weak reference and ends together with the environment.
fn spawn_maintenance(inner: &Arc<Inner>) {
    let sweep_interval = inner.sweep_interval;
    let inner = Arc::downgrade(inner);

    crate::util::spawn(async move {
        loop {
            tokio::time::sleep(sweep_interval).await;

            let Some(inner) = inner.upgrade() else {
                return;
            };
            inner.libraries.run_pending_tasks().await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test::{test_expression_cache_config, test_expression_environment};
    use geoengine_expression::{DataType, ExpressionParser, Parameter};
    use std::sync::Weak;

    fn parse(expression: &str) -> ExpressionAst {
        ExpressionParser::new(&[Parameter::Number("A".into())], DataType::Number)
            .unwrap()
            .parse("expression", expression)
            .unwrap()
    }

    /// Unloading happens in the background, so this waits for it.
    async fn assert_unloaded(library: &Weak<LinkedExpression>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while library.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the library should be unloaded");
    }

    #[tokio::test]
    async fn it_shares_the_library_of_equal_expressions() {
        let environment = test_expression_environment();

        let (first, second) = tokio::join!(
            environment.link(parse("A + 1")),
            environment.link(parse("A + 1"))
        );
        let (first, second) = (first.unwrap(), second.unwrap());
        assert!(Arc::ptr_eq(&first, &second));

        let third = environment.link(parse("A + 1")).await.unwrap();
        assert!(Arc::ptr_eq(&first, &third));
    }

    #[tokio::test]
    async fn it_unloads_idle_libraries_without_further_requests() {
        let environment = ExpressionEnvironment::new(ExpressionCacheConfig {
            idle_ttl: Duration::from_millis(50),
            sweep_interval: Duration::from_millis(10),
            ..test_expression_cache_config()
        });

        let library = environment.link(parse("A + 1")).await.unwrap();
        let unloaded = Arc::downgrade(&library);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            unloaded.upgrade().is_some(),
            "a user keeps the library loaded"
        );

        drop(library);

        assert_unloaded(&unloaded).await;
    }

    #[tokio::test]
    async fn it_unloads_libraries_above_the_size_limit() {
        let environment = ExpressionEnvironment::new(ExpressionCacheConfig {
            idle_ttl: Duration::from_hours(1),
            sweep_interval: Duration::from_hours(1),
            max_cache_size_in_bytes: 1,
            ..test_expression_cache_config()
        });

        let library = environment.link(parse("A + 1")).await.unwrap();
        let unloaded = Arc::downgrade(&library);
        drop(library);

        environment.0.libraries.run_pending_tasks().await;

        assert_unloaded(&unloaded).await;
    }

    #[tokio::test]
    async fn it_retries_after_a_failure() {
        let directory = tempfile::tempdir().unwrap();
        let blocker = directory.path().join("blocker");
        std::fs::write(&blocker, "not a directory").unwrap();

        let environment = ExpressionEnvironment::new(ExpressionCacheConfig {
            workspace_path: blocker.join("workspace"),
            ..test_expression_cache_config()
        });

        assert!(environment.link(parse("A + 1")).await.is_err());

        std::fs::remove_file(&blocker).unwrap();

        assert!(environment.link(parse("A + 1")).await.is_ok());
    }

    #[tokio::test]
    async fn it_does_not_block_concurrent_callers_with_a_single_compilation_permit() {
        let environment = ExpressionEnvironment::new(ExpressionCacheConfig {
            max_concurrent_compilations: NonZeroUsize::MIN,
            ..test_expression_cache_config()
        });

        let (first, second) = tokio::join!(
            environment.link(parse("A + 1")),
            environment.link(parse("A + 2"))
        );
        first.unwrap();
        second.unwrap();
    }
}
