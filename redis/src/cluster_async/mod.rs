//! This module provides async functionality for Redis Cluster.
//!
//! By default, [`ClusterConnection`] makes use of [`MultiplexedConnection`] and maintains a pool
//! of connections to each node in the cluster. While it  generally behaves similarly to
//! the sync cluster module, certain commands do not route identically, due most notably to
//! a current lack of support for routing commands to multiple nodes.
//!
//! Also note that pubsub functionality is not currently provided by this module.
//!
//! # Example
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::AsyncCommands;
//!
//! async fn fetch_an_integer() -> String {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     let _: () = connection.set("test", "test_data").await.unwrap();
//!     let rv: String = connection.get("test").await.unwrap();
//!     return rv;
//! }
//! ```

mod connections_container;
use std::{
    collections::HashMap,
    fmt, io,
    iter::Iterator,
    marker::Unpin,
    mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{self, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{self, Poll},
};

#[cfg(not(feature = "tls-rustls"))]
use crate::connection::TlsConnParams;
use crate::{
    aio::{get_socket_addrs, ConnectionLike, MultiplexedConnection},
    cluster::{get_connection_info, slot_cmd},
    cluster_client::{ClusterParams, RetryParams},
    cluster_routing::{
        MultipleNodeRoutingInfo, Redirect, ResponsePolicy, Route, RoutingInfo,
        SingleNodeRoutingInfo, SlotAddr,
    },
    cluster_topology::{
        calculate_topology, DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES,
        DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL, DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT,
        MANAGEMENT_CONN_NAME,
    },
    Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Value,
};
use rand::{thread_rng, Rng};
use std::time::Duration;

#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use crate::aio::{async_std::AsyncStd, RedisRuntime};
use arcstr::ArcStr;
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use backoff_std_async::future::retry;
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use backoff_std_async::{Error, ExponentialBackoff};

#[cfg(feature = "tokio-comp")]
use backoff_tokio::future::retry;
#[cfg(feature = "tokio-comp")]
use backoff_tokio::{Error, ExponentialBackoff};

use dispose::{Disposable, Dispose};
use futures::{
    future::{self, BoxFuture},
    prelude::*,
    ready, stream,
};
use futures_time::{future::FutureExt, task::sleep};
use pin_project_lite::pin_project;
use std::sync::atomic::AtomicBool;
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver},
    RwLock,
};
use tracing::{info, trace, warn};

use self::connections_container::{
    ClusterNode, ConnectionAndIdentifier, ConnectionType, ConnectionsMap,
    Identifier as ConnectionIdentifier,
};

/// This represents an async Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
#[derive(Clone)]
pub struct ClusterConnection<C = MultiplexedConnection>(mpsc::Sender<Message<C>>);

impl<C> ClusterConnection<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    pub(crate) async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<ClusterConnection<C>> {
        ClusterConnInner::new(initial_nodes, cluster_params)
            .await
            .map(|inner| {
                let (tx, mut rx) = mpsc::channel::<Message<_>>(100);
                let stream = async move {
                    let _ = stream::poll_fn(move |cx| rx.poll_recv(cx))
                        .map(Ok)
                        .forward(inner)
                        .await;
                };
                #[cfg(feature = "tokio-comp")]
                tokio::spawn(stream);
                #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                AsyncStd::spawn(stream);

                ClusterConnection(tx)
            })
    }

    /// Send a command to the given `routing`. If `routing` is [None], it will be computed from `cmd`.
    pub async fn route_command(&mut self, cmd: &Cmd, routing: RoutingInfo) -> RedisResult<Value> {
        trace!("route_command");
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(Message {
                cmd: CmdArg::Cmd {
                    cmd: Arc::new(cmd.clone()), // TODO Remove this clone?
                    routing: CommandRouting::Route(routing),
                },
                sender,
            })
            .await
            .map_err(|_| {
                RedisError::from(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "redis_cluster: Unable to send command",
                ))
            })?;
        receiver
            .await
            .unwrap_or_else(|_| {
                Err(RedisError::from(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "redis_cluster: Unable to receive command",
                )))
            })
            .map(|response| match response {
                Response::Single(value) => value,
                Response::Multiple(_) => unreachable!(),
            })
    }

    /// Send commands in `pipeline` to the given `route`. If `route` is [None], it will be computed from `pipeline`.
    pub async fn route_pipeline<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
        route: SingleNodeRoutingInfo,
    ) -> RedisResult<Vec<Value>> {
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(Message {
                cmd: CmdArg::Pipeline {
                    pipeline: Arc::new(pipeline.clone()), // TODO Remove this clone?
                    offset,
                    count,
                    route,
                },
                sender,
            })
            .await
            .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))?;

        receiver
            .await
            .unwrap_or_else(|_| Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))))
            .map(|response| match response {
                Response::Multiple(values) => values,
                Response::Single(_) => unreachable!(),
            })
    }
}

type ConnectionFuture<C> = future::Shared<BoxFuture<'static, C>>;
#[doc(hidden)]
pub type AsyncClusterNode<C> = ClusterNode<ConnectionFuture<C>>;
type ConnectionMap<C> = connections_container::ConnectionsMap<ConnectionFuture<C>>;
type ConnectionsContainer<C> =
    self::connections_container::ConnectionsContainer<ConnectionFuture<C>>;

struct InnerCore<C> {
    conn_lock: RwLock<ConnectionsContainer<C>>,
    cluster_params: ClusterParams,
    pending_requests: Mutex<Vec<PendingRequest<Response, C>>>,
    slot_refresh_in_progress: AtomicBool,
}

type Core<C> = Arc<InnerCore<C>>;

struct ClusterConnInner<C> {
    inner: Core<C>,
    state: ConnectionState,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<
        Pin<
            Box<Request<BoxFuture<'static, (OperationTarget, RedisResult<Response>)>, Response, C>>,
        >,
    >,
    refresh_error: Option<RedisError>,
    // A flag indicating the connection's closure and the requirement to shut down all related tasks.
    shutdown_flag: Arc<AtomicBool>,
}

impl<C> Dispose for ClusterConnInner<C> {
    fn dispose(self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone)]
enum CommandRouting<C> {
    Route(RoutingInfo),
    Connection {
        identifier: ConnectionIdentifier,
        conn: ConnectionFuture<C>,
    },
}

#[derive(Clone)]
enum CmdArg<C> {
    Cmd {
        cmd: Arc<Cmd>,
        routing: CommandRouting<C>,
    },
    Pipeline {
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        route: SingleNodeRoutingInfo,
    },
}

fn route_for_pipeline(pipeline: &crate::Pipeline) -> RedisResult<Option<Route>> {
    fn route_for_command(cmd: &Cmd) -> Option<Route> {
        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)) => None,
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(route))) => {
                Some(route)
            }
            Some(RoutingInfo::MultiNode(_)) => None,
            None => None,
        }
    }

    // Find first specific slot and send to it. There's no need to check If later commands
    // should be routed to a different slot, since the server will return an error indicating this.
    pipeline.cmd_iter().map(route_for_command).try_fold(
        None,
        |chosen_route, next_cmd_route| match (chosen_route, next_cmd_route) {
            (None, _) => Ok(next_cmd_route),
            (_, None) => Ok(chosen_route),
            (Some(chosen_route), Some(next_cmd_route)) => {
                if chosen_route.slot() != next_cmd_route.slot() {
                    Err((ErrorKind::CrossSlot, "Received crossed slots in pipeline").into())
                } else if chosen_route.slot_addr() == SlotAddr::ReplicaOptional {
                    Ok(Some(next_cmd_route))
                } else {
                    Ok(Some(chosen_route))
                }
            }
        },
    )
}
enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

enum OperationTarget {
    Node { identifier: ConnectionIdentifier },
    FanOut,
}

impl From<ConnectionIdentifier> for OperationTarget {
    fn from(identifier: ConnectionIdentifier) -> Self {
        OperationTarget::Node { identifier }
    }
}

struct Message<C> {
    cmd: CmdArg<C>,
    sender: oneshot::Sender<RedisResult<Response>>,
}

enum RecoverFuture {
    RecoverSlots(BoxFuture<'static, RedisResult<()>>),
    Reconnect(BoxFuture<'static, Vec<ConnectionIdentifier>>),
}

enum ConnectionState {
    PollComplete,
    Recover(RecoverFuture),
}

impl fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConnectionState::PollComplete => "PollComplete",
                ConnectionState::Recover(_) => "Recover",
            }
        )
    }
}

enum TopologyRefresh {
    Required,
    Unrequired,
}

#[derive(Clone)]
struct RequestInfo<C> {
    cmd: CmdArg<C>,
    redirect: Option<Redirect>,
}

pin_project! {
    #[project = RequestStateProj]
    enum RequestState<F> {
        None,
        Future {
            #[pin]
            future: F,
        },
        Sleep {
            #[pin]
            sleep: BoxFuture<'static, ()>,
        },
    }
}

struct PendingRequest<I, C> {
    retry: u32,
    sender: oneshot::Sender<RedisResult<I>>,
    info: RequestInfo<C>,
}

pin_project! {
    struct Request<F, I, C> {
        retry_params: RetryParams,
        request: Option<PendingRequest<I, C>>,
        #[pin]
        future: RequestState<F>,
    }
}

#[must_use]
enum Next<I, C> {
    Retry {
        request: PendingRequest<I, C>,
    },
    Reconnect {
        request: PendingRequest<I, C>,
        target: ConnectionIdentifier,
    },
    RefreshSlots {
        request: PendingRequest<I, C>,
    },
    Done,
}

impl<F, I, C> Future for Request<F, I, C>
where
    F: Future<Output = (OperationTarget, RedisResult<I>)>,
{
    type Output = Next<I, C>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() {
            return Poll::Ready(Next::Done);
        }
        let future = match this.future.as_mut().project() {
            RequestStateProj::Future { future } => future,
            RequestStateProj::Sleep { sleep } => {
                ready!(sleep.poll(cx));
                return Next::Retry {
                    request: self.project().request.take().unwrap(),
                }
                .into();
            }
            _ => panic!("Request future must be Some"),
        };
        match ready!(future.poll(cx)) {
            (_, Ok(item)) => {
                self.respond(Ok(item));
                Next::Done.into()
            }
            (target, Err(err)) => {
                let identifier = match target {
                    OperationTarget::Node { identifier } => identifier,
                    OperationTarget::FanOut => {
                        trace!("Request error `{}` multi-node request", err);
                        // Fanout operation are retried per internal request, and don't need additional retries.
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                };

                let request = this.request.as_mut().unwrap();
                trace!("Request error `{}` on node `{:?}", err, identifier);

                if request.retry >= this.retry_params.number_of_retries {
                    self.respond(Err(err));
                    return Next::Done.into();
                }
                request.retry = request.retry.saturating_add(1);

                match err.kind() {
                    ErrorKind::Ask => {
                        let mut request = this.request.take().unwrap();
                        request.info.redirect = err
                            .redirect_node()
                            .map(|(node, _slot)| Redirect::Ask(node.to_string()));
                        Next::Retry { request }.into()
                    }
                    ErrorKind::Moved => {
                        let mut request = this.request.take().unwrap();
                        request.info.redirect = err
                            .redirect_node()
                            .map(|(node, _slot)| Redirect::Moved(node.to_string()));
                        Next::RefreshSlots { request }.into()
                    }
                    ErrorKind::TryAgain | ErrorKind::ClusterDown => {
                        // Sleep and retry.
                        let sleep_duration = this.retry_params.wait_time_for_retry(request.retry);
                        this.future.set(RequestState::Sleep {
                            #[cfg(feature = "tokio-comp")]
                            sleep: Box::pin(tokio::time::sleep(sleep_duration)),

                            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                            sleep: Box::pin(async_std::task::sleep(sleep_duration)),
                        });
                        self.poll(cx)
                    }
                    ErrorKind::IoError => {
                        warn!("disconnected from {:?}", identifier);
                        Next::Reconnect {
                            request: this.request.take().unwrap(),
                            target: identifier,
                        }
                        .into()
                    }
                    _ => {
                        if err.is_retryable() {
                            Next::Retry {
                                request: this.request.take().unwrap(),
                            }
                            .into()
                        } else {
                            self.respond(Err(err));
                            Next::Done.into()
                        }
                    }
                }
            }
        }
    }
}

impl<F, I, C> Request<F, I, C>
where
    F: Future<Output = (OperationTarget, RedisResult<I>)>,
{
    fn respond(self: Pin<&mut Self>, msg: RedisResult<I>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .project()
            .request
            .take()
            .expect("Result should only be sent once")
            .sender
            .send(msg);
    }
}

enum ConnectionCheck<C> {
    Found((ConnectionIdentifier, ConnectionFuture<C>)),
    OnlyAddress(String),
    Nothing,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RefreshConnectionType {
    // Refresh only user connections
    OnlyUserConnection,
    // Refresh only management connections
    OnlyManagementConnection,
    // Refresh all connections: both management and user connections.
    AllConnections,
}

impl<C> ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<Disposable<Self>> {
        let connections = Self::create_initial_connections(initial_nodes, &cluster_params).await?;
        let topology_checks_interval = cluster_params.topology_checks_interval;
        let inner = Arc::new(InnerCore {
            conn_lock: RwLock::new(ConnectionsContainer::new(
                Default::default(),
                connections,
                cluster_params.read_from_replicas,
                0,
            )),
            cluster_params,
            pending_requests: Mutex::new(Vec::new()),
            slot_refresh_in_progress: AtomicBool::new(false),
        });
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let connection = ClusterConnInner {
            inner,
            in_flight_requests: Default::default(),
            refresh_error: None,
            state: ConnectionState::PollComplete,
            shutdown_flag: shutdown_flag.clone(),
        };
        Self::refresh_slots_with_retries(connection.inner.clone()).await?;
        if let Some(duration) = topology_checks_interval {
            let periodic_checks_task = ClusterConnInner::periodic_topology_check(
                connection.inner.clone(),
                duration,
                shutdown_flag,
            );
            #[cfg(feature = "tokio-comp")]
            tokio::spawn(periodic_checks_task);
            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            AsyncStd::spawn(periodic_checks_task);
        }

        Ok(Disposable::new(connection))
    }

    /// Go through each of the initial nodes and attempt to retrieve all IP entries from them.
    /// If there's a DNS endpoint that directs to several IP addresses, add all addresses to the initial nodes list.
    /// Returns a vector of tuples, each containing a node's address (including the hostname) and its corresponding SocketAddr if retrieved.
    pub(crate) async fn try_to_expand_initial_nodes(
        initial_nodes: &[ConnectionInfo],
    ) -> Vec<(String, Option<SocketAddr>)> {
        stream::iter(initial_nodes)
            .fold(
                Vec::with_capacity(initial_nodes.len()),
                |mut acc, info| async {
                    let (host, port) = match &info.addr {
                        crate::ConnectionAddr::Tcp(host, port) => (host, port),
                        crate::ConnectionAddr::TcpTls {
                            host,
                            port,
                            insecure: _,
                            tls_params: _,
                        } => (host, port),
                        crate::ConnectionAddr::Unix(_) => {
                            // We don't support multiple addresses for a Unix address. Store the initial node address and continue
                            acc.push((info.addr.to_string(), None));
                            return acc;
                        }
                    };
                    match get_socket_addrs(host, *port).await {
                        Ok(socket_addrs) => {
                            for addr in socket_addrs {
                                acc.push((info.addr.to_string(), Some(addr)));
                            }
                        }
                        Err(_) => {
                            // Couldn't find socket addresses, store the initial node address and continue
                            acc.push((info.addr.to_string(), None));
                        }
                    };
                    acc
                },
            )
            .await
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        params: &ClusterParams,
    ) -> RedisResult<ConnectionMap<C>> {
        let initial_nodes: Vec<(String, Option<SocketAddr>)> =
            Self::try_to_expand_initial_nodes(initial_nodes).await;
        let connections = stream::iter(initial_nodes.iter().cloned())
            .map(|(node_addr, socket_addr)| {
                let params: ClusterParams = params.clone();
                async move {
                    let result = connect_and_check(
                        &node_addr,
                        params,
                        socket_addr,
                        RefreshConnectionType::AllConnections,
                        None,
                    )
                    .await;
                    let node_identifier = if let Some(socket_addr) = socket_addr {
                        socket_addr.to_string()
                    } else {
                        node_addr
                    };
                    result.map(|node| (node_identifier, node))
                }
            })
            .buffer_unordered(initial_nodes.len())
            .fold(
                (
                    ConnectionsMap(HashMap::with_capacity(initial_nodes.len())),
                    None,
                ),
                |mut connections: (ConnectionMap<C>, Option<String>), addr_conn_res| async move {
                    match addr_conn_res {
                        Ok((addr, node)) => {
                            connections.0 .0.insert(addr.into(), node);
                            (connections.0, None)
                        }
                        Err(e) => (connections.0, Some(e.to_string())),
                    }
                },
            )
            .await;
        if connections.0 .0.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "Failed to create initial connections",
                connections.1.unwrap_or("".to_string()),
            )));
        }
        info!("Connected to initial nodes:\n{}", connections.0);
        Ok(connections.0)
    }

    async fn refresh_connections(
        inner: Arc<InnerCore<C>>,
        identifiers: Vec<ConnectionIdentifier>,
        conn_type: RefreshConnectionType,
    ) -> Vec<connections_container::Identifier> {
        info!("Started refreshing connections to {:?}", identifiers);
        let mut connections_container = inner.conn_lock.write().await;
        let cluster_params = &inner.cluster_params;
        let (_, failed_connections) = stream::iter(identifiers.into_iter())
            .fold(
                (&mut *connections_container, Vec::new()),
                |(connections_container, mut failed_connections), identifier| async move {
                    let addr_option = connections_container.address_for_identifier(&identifier);
                    let node_option = connections_container.remove_node(&identifier);
                    if let Some(addr) = addr_option {
                        let conn_type = if node_option.is_none()
                            && conn_type == RefreshConnectionType::OnlyUserConnection
                        {
                            // If we do not have a relevant node for this address, it indicates that it's a new node. All connection types should be refreshed.
                            RefreshConnectionType::AllConnections
                        } else if node_option.is_none()
                            && conn_type == RefreshConnectionType::OnlyManagementConnection
                        {
                            // If we do not have a relevant node for this address and it was called from management connection job,
                            // it indicates that this node has already been removed from the connections container and we shall not create a management connection for it.
                            info!(
                                "Node {:?} were already removed from the connection map,
                                            skipping management connection creation",
                                addr.to_string()
                            );
                            return (connections_container, failed_connections);
                        } else {
                            conn_type
                        };
                        let conn = Self::get_or_create_conn(
                            &addr,
                            cluster_params,
                            conn_type,
                            node_option.clone(),
                        )
                        .await;
                        if let Ok(node) = conn {
                            if node.management_connection.is_none()
                                && conn_type != RefreshConnectionType::OnlyUserConnection
                            {
                                // The node was returned only with a user connection. Add this identifier to the failed list.
                                failed_connections.push(identifier);
                            };
                            connections_container.replace_or_add_connection_for_address(addr, node);
                        } else {
                            failed_connections.push(identifier);
                        }
                    }
                    (connections_container, failed_connections)
                },
            )
            .await;
        info!("refresh connections completed");
        failed_connections
    }

    async fn aggregate_results(
        receivers: Vec<(ArcStr, oneshot::Receiver<RedisResult<Response>>)>,
        routing: &MultipleNodeRoutingInfo,
        response_policy: Option<ResponsePolicy>,
    ) -> RedisResult<Value> {
        let extract_result = |response| match response {
            Response::Single(value) => value,
            Response::Multiple(_) => unreachable!(),
        };

        let convert_result = |res: Result<RedisResult<Response>, _>| {
            res.map_err(|_| RedisError::from((ErrorKind::ResponseError, "request wasn't handled due to internal failure"))) // this happens only if the result sender is dropped before usage.
            .and_then(|res| res.map(extract_result))
        };

        let get_receiver = |(_, receiver): (_, oneshot::Receiver<RedisResult<Response>>)| async {
            convert_result(receiver.await)
        };

        // TODO - once Value::Error will be merged, these will need to be updated to handle this new value.
        match response_policy {
            Some(ResponsePolicy::AllSucceeded) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .map(|mut results| results.pop().unwrap()) // unwrap is safe, since at least one function succeeded
            }
            Some(ResponsePolicy::OneSucceeded) => future::select_ok(
                receivers
                    .into_iter()
                    .map(|tuple| Box::pin(get_receiver(tuple))),
            )
            .await
            .map(|(result, _)| result),
            Some(ResponsePolicy::OneSucceededNonEmpty) => {
                future::select_ok(receivers.into_iter().map(|(_, receiver)| {
                    Box::pin(async move {
                        let result = convert_result(receiver.await)?;
                        match result {
                            Value::Nil => Err((ErrorKind::ResponseError, "no value found").into()),
                            _ => Ok(result),
                        }
                    })
                }))
                .await
                .map(|(result, _)| result)
            }
            Some(ResponsePolicy::Aggregate(op)) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| crate::cluster_routing::aggregate(results, op))
            }
            Some(ResponsePolicy::AggregateLogical(op)) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| crate::cluster_routing::logical_aggregate(results, op))
            }
            Some(ResponsePolicy::CombineArrays) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| match routing {
                        MultipleNodeRoutingInfo::MultiSlot(vec) => {
                            crate::cluster_routing::combine_and_sort_array_results(
                                results,
                                vec.iter().map(|(_, indices)| indices),
                            )
                        }
                        _ => crate::cluster_routing::combine_array_results(results),
                    })
            }
            Some(ResponsePolicy::Special) | None => {
                // This is our assumption - if there's no coherent way to aggregate the responses, we just map each response to the sender, and pass it to the user.
                // TODO - once Value::Error is merged, we can use join_all and report separate errors and also pass successes.
                future::try_join_all(receivers.into_iter().map(|(addr, receiver)| async move {
                    let result = convert_result(receiver.await)?;
                    Ok((Value::BulkString(addr.as_bytes().to_vec()), result))
                }))
                .await
                .map(Value::Map)
            }
        }
    }

    // Query a node to discover slot-> master mappings with retries
    async fn refresh_slots_with_retries(inner: Arc<InnerCore<C>>) -> RedisResult<()> {
        if inner
            .slot_refresh_in_progress
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let retry_strategy = ExponentialBackoff {
            initial_interval: DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL,
            max_interval: DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT,
            ..Default::default()
        };
        let retries_counter = AtomicUsize::new(0);
        let res = retry(retry_strategy, || {
            let curr_retry = retries_counter.fetch_add(1, atomic::Ordering::Relaxed);
            Self::refresh_slots(inner.clone(), curr_retry).map_err(Error::from)
        })
        .await;
        inner
            .slot_refresh_in_progress
            .store(false, Ordering::Relaxed);
        res
    }

    async fn periodic_topology_check(
        inner: Arc<InnerCore<C>>,
        interval_duration: Duration,
        shutdown_flag: Arc<AtomicBool>,
    ) {
        const MAX_JITTER_MILLI: u64 = 1000;
        let mut jitter: u64;
        let mut pending_reconnection_identifiers = Vec::new();
        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                return;
            }
            // To avoid server overload or connection storms when multiple clients are present, we'll add a jitter to the interval duration
            {
                let mut rng: rand::prelude::ThreadRng = thread_rng();
                jitter = rng.gen_range(0..MAX_JITTER_MILLI);
            }
            let interval_duration = interval_duration + Duration::from_millis(jitter);
            let _ = sleep(interval_duration.into()).await;

            let retry_strategy = ExponentialBackoff {
                initial_interval: DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL,
                max_interval: DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT,
                ..Default::default()
            };
            let topology_check_res = retry(retry_strategy, || {
                Self::check_for_topology_diff(inner.clone()).map_err(Error::from)
            })
            .await;
            match topology_check_res {
                Ok((refresh_state, mut failed_connections)) => match refresh_state {
                    TopologyRefresh::Required => {
                        info!("Topology change found, starting to refresh slots");
                        let _ = Self::refresh_slots_with_retries(inner.clone()).await;
                    }
                    TopologyRefresh::Unrequired => {
                        failed_connections.append(&mut pending_reconnection_identifiers);
                        if !failed_connections.is_empty() {
                            info!(
                                "Trying to reconnect the following identifiers: {:?}",
                                failed_connections,
                            );
                            let new_failed_identifiers = Self::refresh_connections(
                                inner.clone(),
                                failed_connections,
                                RefreshConnectionType::OnlyManagementConnection,
                            )
                            .await;
                            if !new_failed_identifiers.is_empty() {
                                warn!("The periodic check failed to reconnect the following identifers: {:?}", new_failed_identifiers);
                            }
                            pending_reconnection_identifiers = new_failed_identifiers;
                        }
                    }
                },
                Err(err) => warn!(
                    "The periodic topology check failed to get topology with error: {:?}",
                    err
                ),
            }
        }
    }

    /// Queries `num_of_nodes` random nodes for their topology views.
    /// Returns a tuple consisting of:
    /// 1. A vector containing the successful topology results
    /// 2. A vector that contains identifiers of nodes that experienced connection failures during the query
    async fn get_topology_values_and_failed_conn(
        conn_container_guard: &tokio::sync::RwLockReadGuard<'_, ConnectionsContainer<C>>,
        num_of_nodes: usize,
        conn_type: ConnectionType,
    ) -> (Vec<Value>, Vec<ConnectionIdentifier>) {
        let requested_nodes = conn_container_guard.random_connections(num_of_nodes, conn_type);
        let topology_join_results =
            futures::future::join_all(requested_nodes.map(|node| async move {
                let mut conn: C = node.1.await;
                (node.0, conn.req_packed_command(&slot_cmd()).await)
            }))
            .await;
        let mut topology_values: Vec<_> = vec![];
        let mut failed_connections: Vec<_> = vec![];
        for (identifier, result) in topology_join_results.into_iter() {
            match result {
                Ok(res) => topology_values.push(res),
                Err(err) => {
                    warn!("Received an error while attempting to retrieve the topology view of connection with identifier {:?}:\n{:?}", identifier, err);
                    if err.is_connection_dropped() {
                        failed_connections.push(identifier);
                    }
                }
            }
        }
        (topology_values, failed_connections)
    }

    /// Queries log2n nodes (where n represents the number of cluster nodes) to determine whether their
    /// topology view differs from the one currently stored in the connection manager.
    /// returns a tuple containing two elements: a boolean indicating whether a change was detected, and a vector containing identifiers of failed connections.
    async fn check_for_topology_diff(
        inner: Arc<InnerCore<C>>,
    ) -> RedisResult<(TopologyRefresh, Vec<ConnectionIdentifier>)> {
        let read_guard = inner.conn_lock.read().await;
        // TODO: Starting from Rust V1.67, integers has logarithms support.
        // When we no longer need to support Rust versions < 1.67, remove fast_math and transition to the ilog2 function.
        let num_of_nodes_to_query =
            std::cmp::max(fast_math::log2_raw(read_guard.len() as f32) as usize, 1);
        let (topology_values, failed_connections) = Self::get_topology_values_and_failed_conn(
            &read_guard,
            num_of_nodes_to_query,
            ConnectionType::PreferManagement,
        )
        .await;
        if topology_values.is_empty() && !failed_connections.is_empty() {
            // Couldn't get any topology views and failed connections were found.
            // Return to the caller to refresh failed connections
            return Ok((TopologyRefresh::Unrequired, failed_connections));
        }
        let (_, found_topology_hash) = calculate_topology(
            topology_values,
            DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES,
            inner.cluster_params.tls,
            num_of_nodes_to_query,
            inner.cluster_params.read_from_replicas,
        )?;
        let topology_refresh_state =
            if read_guard.get_current_topology_hash() != found_topology_hash {
                TopologyRefresh::Required
            } else {
                TopologyRefresh::Unrequired
            };
        Ok((topology_refresh_state, failed_connections))
    }

    // Query a node to discover slot-> master mappings
    async fn refresh_slots(inner: Arc<InnerCore<C>>, curr_retry: usize) -> RedisResult<()> {
        info!("refresh_slots started");
        let read_guard = inner.conn_lock.read().await;
        let num_of_nodes = read_guard.len();
        const MAX_REQUESTED_NODES: usize = 50;
        let num_of_nodes_to_query = std::cmp::min(num_of_nodes, MAX_REQUESTED_NODES);
        let (topology_values, failed_connections) = Self::get_topology_values_and_failed_conn(
            &read_guard,
            num_of_nodes_to_query,
            ConnectionType::PreferManagement,
        )
        .await;
        let (new_slots, topology_hash) = match calculate_topology(
            topology_values,
            curr_retry,
            inner.cluster_params.tls,
            num_of_nodes_to_query,
            inner.cluster_params.read_from_replicas,
        ) {
            Ok(res) => res,
            Err(err) => {
                if !failed_connections.is_empty() {
                    // Try to refresh failed connections
                    let _res = ClusterConnInner::refresh_connections(
                        inner.clone(),
                        failed_connections,
                        RefreshConnectionType::OnlyManagementConnection,
                    )
                    .await;
                }
                return Err(err);
            }
        };
        info!("Found slot map: {new_slots}");
        let connections = &*read_guard;
        // Create a new connection vector of the found nodes
        let mut nodes = new_slots.values().flatten().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();
        let nodes_len = nodes.len();
        let addresses_and_connections_iter = stream::iter(nodes)
            .fold(
                Vec::with_capacity(nodes_len),
                |mut addrs_and_conns, addr| async move {
                    if let Some(node) = connections.node_for_address(addr.as_str()) {
                        addrs_and_conns.push((addr, Some(node)));
                        return addrs_and_conns;
                    }
                    // If it's a DNS endpoint, it could have been stored in the existing connections vector using the resolved IP address instead of the DNS endpoint's name.
                    // We shall check if a connection is already exists under the resolved IP name.
                    let (host, port) = match get_host_and_port_from_addr(addr) {
                        Some((host, port)) => (host, port),
                        None => {
                            addrs_and_conns.push((addr, None));
                            return addrs_and_conns;
                        }
                    };
                    let conn = get_socket_addrs(host, port)
                        .await
                        .ok()
                        .map(|mut socket_addresses| {
                            socket_addresses
                                .find_map(|addr| connections.node_for_address(&addr.to_string()))
                        })
                        .unwrap_or(None);
                    addrs_and_conns.push((addr, conn));
                    addrs_and_conns
                },
            )
            .await;
        let new_connections: ConnectionMap<C> = stream::iter(addresses_and_connections_iter)
            .fold(
                ConnectionsMap(HashMap::with_capacity(nodes_len)),
                |mut connections, (addr, node)| async {
                    let conn = Self::get_or_create_conn(
                        addr,
                        &inner.cluster_params,
                        RefreshConnectionType::AllConnections,
                        node,
                    )
                    .await;
                    if let Ok(node) = conn {
                        connections.0.insert(addr.into(), node);
                    }
                    connections
                },
            )
            .await;

        drop(read_guard);
        info!("refresh_slots found nodes:\n{new_connections}");
        // Replace the current slot map and connection vector with the new ones
        let mut write_guard = inner.conn_lock.write().await;
        *write_guard = ConnectionsContainer::new(
            new_slots,
            new_connections,
            inner.cluster_params.read_from_replicas,
            topology_hash,
        );
        Ok(())
    }

    async fn execute_on_multiple_nodes<'a>(
        cmd: &'a Arc<Cmd>,
        routing: &'a MultipleNodeRoutingInfo,
        core: Core<C>,
        response_policy: Option<ResponsePolicy>,
    ) -> (OperationTarget, RedisResult<Response>) {
        trace!("execute_on_multiple_nodes");
        let connections_container = core.conn_lock.read().await;

        // This function maps the connections to senders & receivers of one-shot channels, and the receivers are mapped to `PendingRequest`s.
        // This allows us to pass the new `PendingRequest`s to `try_request`, while letting `execute_on_multiple_nodes` wait on the receivers
        // for all of the individual requests to complete.
        #[allow(clippy::type_complexity)] // The return value is complex, but indentation and linebreaks make it human readable.
        fn into_channels<C>(
            iterator: impl Iterator<Item = (Arc<Cmd>, ConnectionAndIdentifier<ConnectionFuture<C>>)>,
            connections_container: &ConnectionsContainer<C>,
        ) -> (
            Vec<(ArcStr, Receiver<Result<Response, RedisError>>)>,
            Vec<PendingRequest<Response, C>>,
        ) {
            iterator
                .filter_map(|(cmd, (identifier, conn))| {
                    let (sender, receiver) = oneshot::channel();
                    connections_container
                            .address_for_identifier(&identifier) // TODO - this lookup can be avoided, since address is only needed on Special aggregates.
                            .map(|address| {
                                (
                                    (address, receiver),
                                    PendingRequest {
                                        retry: 0,
                                        sender,
                                        info: RequestInfo {
                                            cmd: CmdArg::Cmd {
                                                cmd,
                                                routing: CommandRouting::Connection {
                                                    identifier,
                                                    conn,
                                                },
                                            },
                                            redirect: None,
                                        },
                                    },
                                )
                            })
                })
                .unzip()
        }

        let (receivers, requests): (Vec<_>, Vec<_>) = match routing {
            MultipleNodeRoutingInfo::AllNodes => into_channels(
                connections_container
                    .all_node_connections()
                    .map(|tuple| (cmd.clone(), tuple)),
                &connections_container,
            ),
            MultipleNodeRoutingInfo::AllMasters => into_channels(
                connections_container
                    .all_primary_connections()
                    .map(|tuple| (cmd.clone(), tuple)),
                &connections_container,
            ),
            MultipleNodeRoutingInfo::MultiSlot(slots) => into_channels(
                // TODO - this filter_map means that missing addresses are silently ignored.
                slots.iter().filter_map(|(route, indices)| {
                    connections_container
                        .connection_for_route(route)
                        .map(|tuple| {
                            let new_cmd = crate::cluster_routing::command_for_multi_slot_indices(
                                cmd.as_ref(),
                                indices.iter(),
                            );
                            (Arc::new(new_cmd), tuple)
                        })
                }),
                &connections_container,
            ),
        };

        drop(connections_container);
        core.pending_requests.lock().unwrap().extend(requests);

        let result = Self::aggregate_results(receivers, routing, response_policy)
            .await
            .map(Response::Single);

        (OperationTarget::FanOut, result)
    }

    async fn try_cmd_request(
        cmd: Arc<Cmd>,
        redirect: Option<Redirect>,
        routing: CommandRouting<C>,
        core: Core<C>,
        asking: bool,
    ) -> (OperationTarget, RedisResult<Response>) {
        let route_option = if redirect.is_some() {
            // if we have a redirect, we don't take info from `routing`.
            // TODO - combine the info in `routing` and `redirect` and `asking` into a single structure, so there won't be this question of which field takes precedence.
            SingleNodeRoutingInfo::Random
        } else {
            match routing {
                // commands that are sent to multiple nodes are handled here.
                CommandRouting::Route(RoutingInfo::MultiNode((
                    multi_node_routing,
                    response_policy,
                ))) => {
                    assert!(!asking);
                    assert!(redirect.is_none());
                    return Self::execute_on_multiple_nodes(
                        &cmd,
                        &multi_node_routing,
                        core,
                        response_policy,
                    )
                    .await;
                }

                // commands that have concrete connections, and don't require redirection, are handled here.
                CommandRouting::Connection { identifier, conn } => {
                    let mut conn = conn.await;
                    let result = conn.req_packed_command(&cmd).await.map(Response::Single);
                    return (identifier.into(), result);
                }
                CommandRouting::Route(RoutingInfo::SingleNode(routing_info)) => routing_info,
            }
        };
        trace!("route request to single node");

        // if we reached this point, we're sending the command only to single node, and we need to find the
        // right connection to the node.
        let (identifier, mut conn) =
            Self::get_connection(redirect, route_option, core, asking).await;
        let result = conn.req_packed_command(&cmd).await.map(Response::Single);
        (identifier.into(), result)
    }

    async fn try_pipeline_request(
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        conn: impl Future<Output = (ConnectionIdentifier, C)>,
    ) -> (OperationTarget, RedisResult<Response>) {
        trace!("try_pipeline_request");
        let (identifier, mut conn) = conn.await;
        let result = conn
            .req_packed_commands(&pipeline, offset, count)
            .await
            .map(Response::Multiple);
        (OperationTarget::Node { identifier }, result)
    }

    async fn try_request(
        info: RequestInfo<C>,
        core: Core<C>,
    ) -> (OperationTarget, RedisResult<Response>) {
        let asking = matches!(&info.redirect, Some(Redirect::Ask(_)));

        match info.cmd {
            CmdArg::Cmd { cmd, routing } => {
                Self::try_cmd_request(cmd, info.redirect, routing, core, asking).await
            }
            CmdArg::Pipeline {
                pipeline,
                offset,
                count,
                route,
            } => {
                Self::try_pipeline_request(
                    pipeline,
                    offset,
                    count,
                    Self::get_connection(info.redirect, route, core, asking),
                )
                .await
            }
        }
    }

    async fn get_connection(
        mut redirect: Option<Redirect>,
        route: SingleNodeRoutingInfo,
        core: Core<C>,
        asking: bool,
    ) -> (ConnectionIdentifier, C) {
        let read_guard = core.conn_lock.read().await;

        let conn_check = match redirect.take() {
            Some(Redirect::Moved(moved_addr)) => read_guard
                .connection_for_address(moved_addr.as_str())
                .map_or(
                    ConnectionCheck::OnlyAddress(moved_addr),
                    ConnectionCheck::Found,
                ),
            Some(Redirect::Ask(ask_addr)) => {
                read_guard.connection_for_address(ask_addr.as_str()).map_or(
                    ConnectionCheck::OnlyAddress(ask_addr),
                    ConnectionCheck::Found,
                )
            }
            None => match route {
                SingleNodeRoutingInfo::SpecificNode(route) => read_guard
                    .connection_for_route(&route)
                    .map_or(ConnectionCheck::Nothing, ConnectionCheck::Found),
                SingleNodeRoutingInfo::Random => ConnectionCheck::Nothing,
            },
        };
        drop(read_guard);

        let addr_conn_option = match conn_check {
            ConnectionCheck::Found((identifier, connection)) => {
                Some((identifier, connection.await))
            }
            ConnectionCheck::OnlyAddress(addr) => {
                match connect_and_check::<C>(
                    &addr,
                    core.cluster_params.clone(),
                    None,
                    RefreshConnectionType::AllConnections,
                    None,
                )
                .await
                {
                    Ok(node) => {
                        let connection_clone = node.user_connection.clone();
                        let mut connections = core.conn_lock.write().await;
                        let identifier =
                            connections.replace_or_add_connection_for_address(addr, node);
                        drop(connections);
                        Some((identifier, connection_clone.await))
                    }
                    Err(_) => None, // TODO - this should probably be handled in another way, not sent to a random connection.
                }
            }
            ConnectionCheck::Nothing => None,
        };

        let (identifier, mut conn) = match addr_conn_option {
            Some(tuple) => tuple,
            None => {
                let read_guard = core.conn_lock.read().await;
                let (random_identifier, random_conn_future) = read_guard
                    .random_connections(1, ConnectionType::User)
                    .next()
                    .unwrap(); // TODO - this can panic. handle None.
                drop(read_guard);
                (random_identifier, random_conn_future.await)
            }
        };
        if asking {
            let _ = conn.req_packed_command(&crate::cmd::cmd("ASKING")).await;
        }
        (identifier, conn)
    }

    fn poll_recover(
        &mut self,
        cx: &mut task::Context<'_>,
        future: RecoverFuture,
    ) -> Poll<Result<(), RedisError>> {
        match future {
            RecoverFuture::RecoverSlots(mut future) => match future.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => {
                    trace!("Recovered!");
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(future));
                    trace!("Recover not ready");
                    Poll::Pending
                }
                Poll::Ready(Err(err)) => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                        Self::refresh_slots_with_retries(self.inner.clone()),
                    )));
                    Poll::Ready(Err(err))
                }
            },
            RecoverFuture::Reconnect(mut future) => match future.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    trace!("Reconnected connections");
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(future));
                    trace!("Recover not ready");
                    Poll::Pending
                }
            },
        }
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        let mut pending_requests_guard = self.inner.pending_requests.lock().unwrap();
        if !pending_requests_guard.is_empty() {
            let mut pending_requests = mem::take(&mut *pending_requests_guard);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambigous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = Self::try_request(request.info.clone(), self.inner.clone()).boxed();
                self.in_flight_requests.push(Box::pin(Request {
                    retry_params: self.inner.cluster_params.retry_params.clone(),
                    request: Some(request),
                    future: RequestState::Future { future },
                }));
            }
            *pending_requests_guard = pending_requests;
        }
        drop(pending_requests_guard);

        loop {
            let result = match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                Poll::Ready(Some(result)) => result,
                Poll::Ready(None) | Poll::Pending => break,
            };
            match result {
                Next::Done => {}
                Next::Retry { request } => {
                    let future = Self::try_request(request.info.clone(), self.inner.clone());
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::RefreshSlots { request } => {
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::RebuildSlots);
                    let future = Self::try_request(request.info.clone(), self.inner.clone());
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::Reconnect {
                    request, target, ..
                } => {
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::Reconnect(vec![target]));
                    self.inner.pending_requests.lock().unwrap().push(request);
                }
            }
        }

        match poll_flush_action {
            PollFlushAction::None => {
                if self.in_flight_requests.is_empty() {
                    Poll::Ready(poll_flush_action)
                } else {
                    Poll::Pending
                }
            }
            rebuild @ PollFlushAction::RebuildSlots => Poll::Ready(rebuild),
            reestablish @ PollFlushAction::Reconnect(_) => Poll::Ready(reestablish),
        }
    }

    fn send_refresh_error(&mut self) {
        if self.refresh_error.is_some() {
            if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                .iter_pin_mut()
                .find(|request| request.request.is_some())
            {
                (*request)
                    .as_mut()
                    .respond(Err(self.refresh_error.take().unwrap()));
            } else if let Some(request) = self.inner.pending_requests.lock().unwrap().pop() {
                let _ = request.sender.send(Err(self.refresh_error.take().unwrap()));
            }
        }
    }

    async fn get_or_create_conn(
        addr: &str,
        params: &ClusterParams,
        conn_type: RefreshConnectionType,
        node: Option<AsyncClusterNode<C>>,
    ) -> RedisResult<AsyncClusterNode<C>> {
        if let Some(node) = node {
            if let Some(ref ip) = node.ip {
                if has_dns_changed(addr, ip).await {
                    return connect_and_check(
                        addr,
                        params.clone(),
                        None,
                        RefreshConnectionType::AllConnections,
                        None,
                    )
                    .await;
                }
            };
            match check_node_connections(&node, params, conn_type).await {
                None => Ok(node),
                Some(conn_type) => {
                    connect_and_check(addr, params.clone(), None, conn_type, Some(node)).await
                }
            }
        } else {
            connect_and_check(addr, params.clone(), None, conn_type, None).await
        }
    }
}

enum PollFlushAction {
    None,
    RebuildSlots,
    Reconnect(Vec<ConnectionIdentifier>),
}

impl PollFlushAction {
    fn change_state(self, next_state: PollFlushAction) -> PollFlushAction {
        match self {
            Self::None => next_state,
            rebuild @ Self::RebuildSlots => rebuild,
            Self::Reconnect(mut addrs) => match next_state {
                rebuild @ Self::RebuildSlots => rebuild,
                Self::Reconnect(new_addrs) => {
                    addrs.extend(new_addrs);
                    Self::Reconnect(addrs)
                }
                Self::None => Self::Reconnect(addrs),
            },
        }
    }
}

impl<C> Sink<Message<C>> for Disposable<ClusterConnInner<C>>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    type Error = ();

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        match mem::replace(&mut self.state, ConnectionState::PollComplete) {
            ConnectionState::PollComplete => Poll::Ready(Ok(())),
            ConnectionState::Recover(future) => {
                match ready!(self.as_mut().poll_recover(cx, future)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        // We failed to reconnect, while we will try again we will report the
                        // error if we can to avoid getting trapped in an infinite loop of
                        // trying to reconnect
                        if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                            .iter_pin_mut()
                            .find(|request| request.request.is_some())
                        {
                            (*request).as_mut().respond(Err(err));
                        } else {
                            self.refresh_error = Some(err);
                        }
                        Poll::Ready(Ok(()))
                    }
                }
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        let Message { cmd, sender } = msg;

        let redirect = None;
        let info = RequestInfo { cmd, redirect };

        self.inner
            .pending_requests
            .lock()
            .unwrap()
            .push(PendingRequest {
                retry: 0,
                sender,
                info,
            });
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_complete: {:?}", self.state);
        loop {
            self.send_refresh_error();

            match mem::replace(&mut self.state, ConnectionState::PollComplete) {
                ConnectionState::Recover(future) => {
                    match ready!(self.as_mut().poll_recover(cx, future)) {
                        Ok(()) => (),
                        Err(err) => {
                            // We failed to reconnect, while we will try again we will report the
                            // error if we can to avoid getting trapped in an infinite loop of
                            // trying to reconnect
                            self.refresh_error = Some(err);

                            // Give other tasks a chance to progress before we try to recover
                            // again. Since the future may not have registered a wake up we do so
                            // now so the task is not forgotten
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
                ConnectionState::PollComplete => match ready!(self.poll_complete(cx)) {
                    PollFlushAction::None => return Poll::Ready(Ok(())),
                    PollFlushAction::RebuildSlots => {
                        self.state =
                            ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                                ClusterConnInner::refresh_slots_with_retries(self.inner.clone()),
                            )));
                    }
                    PollFlushAction::Reconnect(identifiers) => {
                        self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                            ClusterConnInner::refresh_connections(
                                self.inner.clone(),
                                identifiers,
                                RefreshConnectionType::OnlyUserConnection,
                            ),
                        )));
                    }
                },
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // Try to drive any in flight requests to completion
        match self.poll_complete(cx) {
            Poll::Ready(poll_flush_action) => match poll_flush_action {
                PollFlushAction::None => (),
                _ => Err(()).map_err(|_| ())?,
            },
            Poll::Pending => (),
        };
        // If we no longer have any requests in flight we are done (skips any reconnection
        // attempts)
        if self.in_flight_requests.is_empty() {
            return Poll::Ready(Ok(()));
        }

        self.poll_flush(cx)
    }
}

impl<C> ConnectionLike for ClusterConnection<C>
where
    C: ConnectionLike + Send + Clone + Unpin + Sync + Connect + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let routing = RoutingInfo::for_routable(cmd)
            .unwrap_or(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random));
        self.route_command(cmd, routing).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let route = route_for_pipeline(pipeline)?;
            self.route_pipeline(pipeline, offset, count, route.into())
                .await
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        0
    }
}
/// Implements the process of connecting to a Redis server
/// and obtaining a connection handle.
pub trait Connect: Sized {
    /// Connect to a node.
    /// For TCP connections, returning a tuple of handle for command execution and the node's IP address.
    /// For UNIX connections, returning a tuple of handle for command execution and None.
    fn connect<'a, T>(
        info: T,
        socket_addr: Option<SocketAddr>,
    ) -> RedisFuture<'a, (Self, Option<IpAddr>)>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for MultiplexedConnection {
    fn connect<'a, T>(
        info: T,
        socket_addr: Option<SocketAddr>,
    ) -> RedisFuture<'a, (MultiplexedConnection, Option<IpAddr>)>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;

            #[cfg(feature = "tokio-comp")]
            return client
                .get_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>(socket_addr)
                .await;

            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            return client
                .get_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>(
                    socket_addr,
                )
                .await;
        }
        .boxed()
    }
}

/// Return true if a DNS change is detected, otherwise return false.
/// This function takes a node's address, examines if its host has encountered a DNS change, where the node's endpoint now leads to a different IP address.
/// If no socket addresses are discovered for the node's host address, or if it's a non-DNS address, it returns false.
/// In case the node's host address resolves to socket addresses and none of them match the current connection's IP,
/// a DNS change is detected, so the current connection isn't valid anymore and a new connection should be made.
async fn has_dns_changed(addr: &str, curr_ip: &IpAddr) -> bool {
    let (host, port) = match get_host_and_port_from_addr(addr) {
        Some((host, port)) => (host, port),
        None => return false,
    };
    let mut updated_addresses = match get_socket_addrs(host, port).await {
        Ok(socket_addrs) => socket_addrs,
        Err(_) => return false,
    };

    !updated_addresses.any(|socket_addr| socket_addr.ip() == *curr_ip)
}

fn warn_mismatch_ip(addr: &str, new_ip: Option<IpAddr>, prev_ip: Option<IpAddr>) {
    warn!(
        "New IP was found for node {:?}: 
                new connection IP = {:?}, previous connection IP = {:?}",
        addr, new_ip, prev_ip
    );
}

fn warn_management_conn_faild(addr: &str, err: RedisError) {
    warn!(
        "Failed to create management connection for node {:?}. Error: {:?}",
        addr, err
    );
}

fn create_async_node<C>(
    user_conn: C,
    management_conn: Option<C>,
    ip: Option<IpAddr>,
) -> AsyncClusterNode<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    let user_conn = async { user_conn }.boxed().shared();
    let management_conn =
        management_conn.map(|management_conn| async { management_conn }.boxed().shared());

    AsyncClusterNode::new(user_conn, management_conn, ip)
}

async fn connect_and_check_all_connections<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<AsyncClusterNode<C>>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match future::join(
        create_connection(addr, params.clone(), socket_addr),
        create_connection(addr, params.clone(), socket_addr),
    )
    .await
    {
        (Ok(conn_1), Ok(conn_2)) => {
            // Both connections were successfully established
            let (mut user_conn, user_ip): (C, Option<IpAddr>) = conn_1;
            let (mut management_conn, management_ip): (C, Option<IpAddr>) = conn_2;
            if user_ip == management_ip {
                // Set up both connections
                setup_user_connection(&mut user_conn, params).await?;
                // If the setup of the management connection fails, set it as a None
                let management_conn = setup_management_connection(&mut management_conn)
                    .await
                    .ok()
                    .map(|_| management_conn);
                Ok(create_async_node(user_conn, management_conn, user_ip))
            } else {
                // Use only the connection with the latest IP address
                warn_mismatch_ip(addr, user_ip, management_ip);
                if has_dns_changed(addr, &user_ip.unwrap()).await {
                    // The user_ip is incorrect. Use the created `management_conn` for the user connection
                    user_conn = management_conn;
                    setup_user_connection(&mut user_conn, params).await?;
                    Ok(create_async_node(user_conn, None, management_ip))
                } else {
                    // The user_ip is correct. Use the user connetion and drop the management connection
                    setup_user_connection(&mut user_conn, params).await?;
                    Ok(create_async_node(user_conn, None, user_ip))
                }
            }
        }
        (Ok(conn), Err(err)) | (Err(err), Ok(conn)) => {
            // Only a single connection was successfully established. Use it for the user connection
            warn_management_conn_faild(addr, err);
            let (mut user_conn, user_ip): (C, Option<IpAddr>) = conn;
            setup_user_connection(&mut user_conn, params).await?;
            Ok(create_async_node(user_conn, None, user_ip))
        }
        (Err(err_1), Err(err_2)) => {
            // Neither of the connections succeeded.
            Err(RedisError::from((
                ErrorKind::IoError,
                "Failed to refresh all connections",
                format!(
                    "Node: {:?}.\nRecieved errors: {:?}\n{:?}",
                    addr, err_1, err_2
                ),
            )))
        }
    }
}

async fn connect_and_check_only_management_conn<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    mut node: AsyncClusterNode<C>,
) -> RedisResult<AsyncClusterNode<C>>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match create_connection(addr, params.clone(), socket_addr).await {
        Ok((mut new_conn, new_ip)) => {
            if new_ip == node.ip {
                // The new IP matches the existing one. Use this connection for the management connection.
                setup_management_connection(&mut new_conn).await?;
                node.management_connection = Some(async { new_conn }.boxed().shared());
            } else {
                // An IP mismatch was detected. Attempt to establish a new connection to replace both the management and user connections.
                // Use the successfully established connection for the user, then proceed to create a new one for management.
                warn_mismatch_ip(addr, new_ip, node.ip);
                setup_user_connection(&mut new_conn, params.clone()).await?;
                node.user_connection = async { new_conn }.boxed().shared();
                node.ip = new_ip;
                match create_connection(addr, params, socket_addr).await {
                    Ok((mut management_conn, _ip)) => {
                        match setup_management_connection(&mut management_conn).await {
                            Ok(_) => {
                                // Successfully created and setup a management connection. Set this connection to the node.
                                node.management_connection =
                                    Some(async { management_conn }.boxed().shared())
                            }
                            Err(err) => {
                                // Failed to setup the managment connection
                                warn_management_conn_faild(addr, err);
                                node.management_connection = None;
                            }
                        };
                    }
                    Err(err) => {
                        // Failed to create another connection
                        warn_management_conn_faild(addr, err);
                        node.management_connection = None;
                    }
                }
            }
            Ok(node)
        }
        Err(err) => {
            // Failed to create another connection
            warn_management_conn_faild(addr, err);
            node.management_connection = None;
            Ok(node)
        }
    }
}

#[doc(hidden)]
pub async fn connect_and_check<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    conn_type: RefreshConnectionType,
    node: Option<AsyncClusterNode<C>>,
) -> RedisResult<AsyncClusterNode<C>>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match conn_type {
        RefreshConnectionType::OnlyUserConnection => {
            let (user_conn, ip) = create_user_connection(addr, params.clone(), socket_addr).await?;
            if let Some(node) = node {
                let mut management_conn = match node.management_connection {
                    Some(ref conn) => Some(conn.clone().await),
                    None => None,
                };
                if ip != node.ip {
                    // New IP was found, refresh the management connection too
                    management_conn = create_management_connection(addr, params, socket_addr)
                        .await
                        .ok()
                        .map(|(conn, _ip): (C, Option<IpAddr>)| conn);
                }
                Ok(create_async_node(user_conn, management_conn, ip))
            } else {
                Ok(create_async_node(user_conn, None, ip))
            }
        }
        RefreshConnectionType::OnlyManagementConnection => {
            // Refreshing only the management connection requires the node to exist alongside a user connection. Otherwise, refresh all connections.
            match node {
                Some(node) => {
                    connect_and_check_only_management_conn(addr, params, socket_addr, node).await
                }
                None => connect_and_check_all_connections(addr, params, socket_addr).await,
            }
        }
        RefreshConnectionType::AllConnections => {
            connect_and_check_all_connections(addr, params, socket_addr).await
        }
    }
}

async fn create_user_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let (mut conn, ip): (C, Option<IpAddr>) =
        create_connection(node, params.clone(), socket_addr).await?;
    setup_user_connection(&mut conn, params).await?;
    Ok((conn, ip))
}

async fn create_management_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let (mut conn, ip): (C, Option<IpAddr>) =
        create_connection(node, params.clone(), socket_addr).await?;
    setup_management_connection(&mut conn).await?;
    Ok((conn, ip))
}

async fn setup_user_connection<C>(conn: &mut C, params: ClusterParams) -> RedisResult<()>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas
        != crate::cluster_topology::ReadFromReplicaStrategy::AlwaysFromPrimary;
    let connection_timeout = params.connection_timeout.into();
    check_connection(conn, connection_timeout).await?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        crate::cmd("READONLY").query_async(conn).await?;
    }
    Ok(())
}

async fn setup_management_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    crate::cmd("CLIENT")
        .arg(&["SETNAME", MANAGEMENT_CONN_NAME])
        .query_async(conn)
        .await?;
    Ok(())
}

async fn create_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let connection_timeout: futures_time::time::Duration = params.connection_timeout.into();
    let info = get_connection_info(node, params)?;
    C::connect(info, socket_addr)
        .timeout(connection_timeout)
        .await?
}

/// The function returns None if the checked connection/s are healthy. Otherwise, it returns the type of the unhealthy connection/s.
async fn check_node_connections<C>(
    node: &AsyncClusterNode<C>,
    params: &ClusterParams,
    conn_type: RefreshConnectionType,
) -> Option<RefreshConnectionType>
where
    C: ConnectionLike + Send + 'static + Clone,
{
    let timeout = params.connection_timeout.into();
    let management_conn_state = async {
        match node.management_connection.clone() {
            Some(conn) => {
                let conn_state = check_connection(&mut conn.await, timeout).await;
                if conn_state.is_err() {
                    warn!(
                        "The management connection for node {:?} is unhealthy. Error: {:?}",
                        node, conn_state
                    );
                }
                conn_state
            }
            None => {
                let err_msg = format!("The management connection for node {:?} isn't set", node);
                warn!(err_msg);
                Err(RedisError::from((
                    ErrorKind::NoneManagementConn,
                    "",
                    err_msg,
                )))
            }
        }
    };
    match conn_type {
        RefreshConnectionType::OnlyUserConnection => {
            let mut user_conn = node.user_connection.clone().await;
            match check_connection(&mut user_conn, timeout).await {
                Ok(_) => None,
                Err(_) => Some(RefreshConnectionType::OnlyUserConnection),
            }
        }
        RefreshConnectionType::OnlyManagementConnection => match management_conn_state.await {
            Ok(_) => None,
            Err(_) => Some(RefreshConnectionType::OnlyManagementConnection),
        },
        RefreshConnectionType::AllConnections => {
            let mut user_conn = node.user_connection.clone().await;
            let user_conn_state = check_connection(&mut user_conn, timeout).await;
            match (user_conn_state, management_conn_state.await) {
                (Ok(_), Ok(_)) => None,
                (Ok(_), Err(_)) => Some(RefreshConnectionType::OnlyManagementConnection),
                (Err(err), Ok(_)) => {
                    warn!(
                        "The user connection for node {:?} is unhealthy. Error: {:?}",
                        node, err
                    );
                    Some(RefreshConnectionType::OnlyUserConnection)
                }
                (Err(user_err), Err(management_err)) => {
                    warn!(
                        "Both the management and user connections for node {:?} are unhealthy. Errors:\n{:?}\n{:?}",
                        node, user_err, management_err
                    );
                    Some(RefreshConnectionType::AllConnections)
                }
            }
        }
    }
}

async fn check_connection<C>(conn: &mut C, timeout: futures_time::time::Duration) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    crate::cmd("PING")
        .query_async::<_, String>(conn)
        .timeout(timeout)
        .await??;
    Ok(())
}

/// Splits a string address into host and port. If the passed address cannot be parsed, None is returned.
/// [addr] should be in the following format: "<host>:<port>".
fn get_host_and_port_from_addr(addr: &str) -> Option<(&str, u16)> {
    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let host = parts.first().unwrap();
    let port = parts.get(1).unwrap();
    port.parse::<u16>().ok().map(|port| (*host, port))
}

#[cfg(test)]
mod pipeline_routing_tests {
    use super::route_for_pipeline;
    use crate::{
        cluster_routing::{Route, SlotAddr},
        cmd,
    };

    #[test]
    fn test_first_route_is_found() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .get("foo") // route to slot 12182
            .add_command(cmd("EVAL")); // route randomly

        assert_eq!(
            route_for_pipeline(&pipeline),
            Ok(Some(Route::new(12182, SlotAddr::ReplicaOptional)))
        );
    }

    #[test]
    fn test_return_none_if_no_route_is_found() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .add_command(cmd("EVAL")); // route randomly

        assert_eq!(route_for_pipeline(&pipeline), Ok(None));
    }

    #[test]
    fn test_prefer_primary_route_over_replica() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .get("foo") // route to replica of slot 12182
            .add_command(cmd("FLUSHALL")) // route to all masters
            .add_command(cmd("EVAL"))// route randomly
            .set("foo", "bar"); // route to primary of slot 12182

        assert_eq!(
            route_for_pipeline(&pipeline),
            Ok(Some(Route::new(12182, SlotAddr::Master)))
        );
    }

    #[test]
    fn test_raise_cross_slot_error_on_conflicting_slots() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .set("baz", "bar") // route to slot 4813
            .get("foo"); // route to slot 12182

        assert_eq!(
            route_for_pipeline(&pipeline).unwrap_err().kind(),
            crate::ErrorKind::CrossSlot
        );
    }
}
