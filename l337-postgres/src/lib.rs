//! Postgres adapater for l3-37 pool
// #![deny(missing_docs, missing_debug_implementations)]

extern crate futures;
pub extern crate l337_futures3;
extern crate tokio;
pub extern crate tokio_postgres;

use std::{fmt, future::Future, pin::Pin};

use futures::{
    channel::oneshot,
    compat::{Future01CompatExt, Stream01CompatExt},
    future::{FutureExt, TryFutureExt},
    stream::TryStreamExt,
};
use tokio_postgres::{Client, Error, Socket, tls::{MakeTlsConnect, TlsConnect}};


type Result<T> = std::result::Result<T, Error>;

pub struct AsyncConnection {
    pub client: Client,
    broken: bool,
    receiver: oneshot::Receiver<bool>,
}

/// A `ManageConnection` for `tokio_postgres::Connection`s.
pub struct PostgresConnectionManager<T>
where
    T: MakeTlsConnect<Socket> + Send
{
    config: String,
    tls: T
}

impl<T> PostgresConnectionManager<T>
where
    T: MakeTlsConnect<Socket> + Send
{
    /// Create a new `PostgresConnectionManager`.
    pub fn new(config: &str, tls: T) -> Result<PostgresConnectionManager<T>> {
        Ok(PostgresConnectionManager {
            config: config.to_owned(),
            tls
        })
    }
}

impl<T> l337_futures3::ManageConnection for PostgresConnectionManager<T>
where
    T: MakeTlsConnect<Socket> + 'static + Send + Sync + Clone,
    T::TlsConnect: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Stream: Send
{
    type Connection = AsyncConnection;
    type Error = Error;

    fn connect(
        &self,
    ) -> Pin<Box<Future<Output = std::result::Result<Self::Connection, l337_futures3::Error<Self::Error>>> + Send>>
    {
        tokio_postgres::connect(&self.config, self.tls.clone())
            .compat()
            .map_ok(|(client, connection)| {
                let (sender, receiver) = oneshot::channel();
                tokio::spawn(
                    connection
                        .compat()
                        .map_err(|_| {
                            sender
                                .send(true)
                                .unwrap_or_else(|e| panic!("failed to send shutdown notice: {}", e));
                        })
                        .compat()
                );
                AsyncConnection {
                    broken: false,
                    client,
                    receiver
                }
            })
            .map_err(|e| l337_futures3::Error::External(e))
            .boxed()
    }

    fn is_valid(
        &self,
        mut conn: Self::Connection,
    ) -> Pin<Box<Future<Output = std::result::Result<(), l337_futures3::Error<Self::Error>>> + Send>> {
        // If we can execute this without erroring, we're definitely still connected to the datbase
        conn.client
            .simple_query("")
            .compat()
            .try_collect::<Vec<_>>()
            .map_ok(|_| ())
            .map_err(|e| l337_futures3::Error::External(e))
            .boxed()
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        if conn.broken {
            return true;
        }

        match conn.receiver.try_recv() {
            // If we get any message, the connection task stopped, which means this connection is
            // now dead
            Ok(Some(_)) => {
                conn.broken = true;
                true
            }
            // If the future isn't ready, then we haven't sent a value which means the future is
            // stil successfully running
            Ok(None) => false,
            // This should never happen, we don't shutdown the future
            Err(err) => panic!("polling oneshot failed: {}", err),
        }
    }

    fn timed_out(&self) -> l337_futures3::Error<Self::Error> {
        unimplemented!()
        // Error::io(io::ErrorKind::TimedOut.into())
    }
}

impl<T> fmt::Debug for PostgresConnectionManager<T>
where T: MakeTlsConnect<Socket> + Send
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use l337_futures3::{Config, Pool};
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::runtime::current_thread::Runtime;
    use tokio_postgres::params::IntoConnectParams;

    #[test]
    fn it_works() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .into_connect_params()
                .unwrap(),
            || tokio_postgres::TlsMode::None,
        ).unwrap();

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        let future = Pool::new(mngr, config).and_then(|pool| {
            pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 1::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(1, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| ((), connection))
                    .map_err(|e| l337_futures3::Error::External(e))
            })
        });

        runtime.block_on(future).expect("could not run");
    }

    #[test]
    fn it_allows_multiple_queries_at_the_same_time() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .into_connect_params()
                .unwrap(),
            || tokio_postgres::TlsMode::None,
        ).unwrap();

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        let future = Pool::new(mngr, config).and_then(|pool| {
            let q1 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 1::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(1, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    }).map_err(|e| l337_futures3::Error::External(e))
            });

            let q2 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 2::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(2, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    }).map_err(|e| l337_futures3::Error::External(e))
            });

            q1.join(q2)
        });

        runtime.block_on(future).expect("could not run");
    }

    #[test]
    fn it_reuses_connections() {
        let mngr = PostgresConnectionManager::new(
            "postgres://pass_user:password@localhost:5433/postgres"
                .into_connect_params()
                .unwrap(),
            || tokio_postgres::TlsMode::None,
        ).unwrap();

        let mut runtime = Runtime::new().expect("could not run");
        let config: Config = Default::default();
        let future = Pool::new(mngr, config).and_then(|pool| {
            let q1 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 1::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(1, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    }).map_err(|e| l337_futures3::Error::External(e))
            });

            let q2 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 2::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(2, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    }).map_err(|e| l337_futures3::Error::External(e))
            });

            let q3 = pool.connection().and_then(|mut conn| {
                conn.client
                    .prepare("SELECT 3::INT4")
                    .and_then(move |select| {
                        conn.client.query(&select, &[]).for_each(|row| {
                            assert_eq!(3, row.get::<_, i32>(0));
                            Ok(())
                        })
                    }).map(|connection| {
                        sleep(Duration::from_secs(5));
                        ((), connection)
                    }).map_err(|e| l337_futures3::Error::External(e))
            });

            q1.join3(q2, q3)
        });

        runtime.block_on(future).expect("could not run");
    }
}
