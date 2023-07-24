//! This module extends the library to support Redis Cluster.
//!
//! Note that this module does not currently provide pubsub
//! functionality.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::ClusterClient;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let _: () = connection.set("test", "test_data").unwrap();
//! let rv: String = connection.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::{cluster_pipe, ClusterClient};
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let key = "test";
//!
//! let _: () = cluster_pipe()
//!     .rpush(key, "123").ignore()
//!     .ltrim(key, -10, -1).ignore()
//!     .expire(key, 60).ignore()
//!     .query(&mut connection).unwrap();
//! ```
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::iter::Iterator;
use std::str::FromStr;
use std::sync::{atomic, Arc};
use std::thread;
use std::time::Duration;

use derivative::Derivative;
use log::trace;
use rand::{seq::IteratorRandom, thread_rng, Rng};

use crate::cluster_pipeline::UNROUTABLE_ERROR;
use crate::cluster_routing::{MultipleNodeRoutingInfo, SingleNodeRoutingInfo, SlotAddr};
use crate::cmd::{cmd, Cmd};
use crate::connection::{
    connect, Connection, ConnectionAddr, ConnectionInfo, ConnectionLike, RedisConnectionInfo,
};
use crate::parser::parse_redis_value;
use crate::types::{ErrorKind, HashMap, RedisError, RedisResult, Value};
use crate::IntoConnectionInfo;
use crate::{
    cluster_client::ClusterParams,
    cluster_routing::{Redirect, Routable, Route, RoutingInfo, Slot, SlotMap, SLOT_SIZE},
};

pub use crate::cluster_client::{ClusterClient, ClusterClientBuilder};
pub use crate::cluster_pipeline::{cluster_pipe, ClusterPipeline};

/// Implements the process of connecting to a Redis server
/// and obtaining and configuring a connection handle.
pub trait Connect: Sized {
    /// Connect to a node, returning handle for command execution.
    fn connect<T>(info: T, timeout: Option<Duration>) -> RedisResult<Self>
    where
        T: IntoConnectionInfo;

    /// Sends an already encoded (packed) command into the TCP socket and
    /// does not read a response.  This is useful for commands like
    /// `MONITOR` which yield multiple items.  This needs to be used with
    /// care because it changes the state of the connection.
    fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()>;

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()>;

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()>;

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    fn recv_response(&mut self) -> RedisResult<Value>;
}

impl Connect for Connection {
    fn connect<T>(info: T, timeout: Option<Duration>) -> RedisResult<Self>
    where
        T: IntoConnectionInfo,
    {
        connect(&info.into_connection_info()?, timeout)
    }

    fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        Self::send_packed_command(self, cmd)
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        Self::set_write_timeout(self, dur)
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        Self::set_read_timeout(self, dur)
    }

    fn recv_response(&mut self) -> RedisResult<Value> {
        Self::recv_response(self)
    }
}

/// This represents a Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
pub struct ClusterConnection<C = Connection> {
    initial_nodes: Vec<ConnectionInfo>,
    connections: RefCell<HashMap<String, C>>,
    slots: RefCell<SlotMap>,
    auto_reconnect: RefCell<bool>,
    read_from_replicas: bool,
    read_timeout: RefCell<Option<Duration>>,
    write_timeout: RefCell<Option<Duration>>,
    cluster_params: ClusterParams,
}

impl<C> ClusterConnection<C>
where
    C: ConnectionLike + Connect,
{
    pub(crate) fn new(
        cluster_params: ClusterParams,
        initial_nodes: Vec<ConnectionInfo>,
    ) -> RedisResult<Self> {
        let connection = Self {
            connections: RefCell::new(HashMap::new()),
            slots: RefCell::new(SlotMap::new()),
            auto_reconnect: RefCell::new(true),
            read_from_replicas: cluster_params.read_from_replicas,
            cluster_params,
            read_timeout: RefCell::new(None),
            write_timeout: RefCell::new(None),
            initial_nodes: initial_nodes.to_vec(),
        };
        connection.create_initial_connections()?;

        Ok(connection)
    }

    /// Set an auto reconnect attribute.
    /// Default value is true;
    pub fn set_auto_reconnect(&self, value: bool) {
        let mut auto_reconnect = self.auto_reconnect.borrow_mut();
        *auto_reconnect = value;
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        if dur.is_some() && dur.unwrap().is_zero() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Duration should be None or non-zero.",
            )));
        }

        let mut t = self.write_timeout.borrow_mut();
        *t = dur;
        let connections = self.connections.borrow();
        for conn in connections.values() {
            conn.set_write_timeout(dur)?;
        }
        Ok(())
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        if dur.is_some() && dur.unwrap().is_zero() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Duration should be None or non-zero.",
            )));
        }

        let mut t = self.read_timeout.borrow_mut();
        *t = dur;
        let connections = self.connections.borrow();
        for conn in connections.values() {
            conn.set_read_timeout(dur)?;
        }
        Ok(())
    }

    /// Check that all connections it has are available (`PING` internally).
    #[doc(hidden)]
    pub fn check_connection(&mut self) -> bool {
        <Self as ConnectionLike>::check_connection(self)
    }

    pub(crate) fn execute_pipeline(&mut self, pipe: &ClusterPipeline) -> RedisResult<Vec<Value>> {
        self.send_recv_and_retry_cmds(pipe.commands())
    }

    /// Returns the connection status.
    ///
    /// The connection is open until any `read_response` call recieved an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    fn create_initial_connections(&self) -> RedisResult<()> {
        let mut connections = HashMap::with_capacity(self.initial_nodes.len());

        for info in self.initial_nodes.iter() {
            let addr = info.addr.to_string();

            if let Ok(mut conn) = self.connect(&addr) {
                if conn.check_connection() {
                    connections.insert(addr, conn);
                    break;
                }
            }
        }

        if connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "It failed to check startup nodes.",
            )));
        }

        *self.connections.borrow_mut() = connections;
        self.refresh_slots()?;
        Ok(())
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) -> RedisResult<()> {
        let mut slots = self.slots.borrow_mut();
        *slots = self.create_new_slots()?;

        let mut nodes = slots.values().flatten().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();

        let mut connections = self.connections.borrow_mut();
        *connections = nodes
            .into_iter()
            .filter_map(|addr| {
                if connections.contains_key(addr) {
                    let mut conn = connections.remove(addr).unwrap();
                    if conn.check_connection() {
                        return Some((addr.to_string(), conn));
                    }
                }

                if let Ok(mut conn) = self.connect(addr) {
                    if conn.check_connection() {
                        return Some((addr.to_string(), conn));
                    }
                }

                None
            })
            .collect();

        Ok(())
    }

    fn create_new_slots(&self) -> RedisResult<SlotMap> {
        let mut connections = self.connections.borrow_mut();
        let mut rng = thread_rng();
        let len = connections.len();
        let mut samples = connections.values_mut().choose_multiple(&mut rng, len);
        let mut new_slots = SlotMap::new();
        let mut result = Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error.",
            "didn't get any slots from server".to_string(),
        )));
        for conn in samples.iter_mut() {
            let value = conn.req_command(&slot_cmd())?;
            match parse_slots(&value, self.cluster_params.tls).and_then(|v| {
                build_slot_map(&mut new_slots, v, self.cluster_params.read_from_replicas)
            }) {
                Ok(_) => {
                    result = Ok(new_slots);
                    break;
                }
                Err(err) => result = Err(err),
            }
        }
        result
    }

    fn connect(&self, node: &str) -> RedisResult<C> {
        let info = get_connection_info(node, self.cluster_params.clone())?;

        let mut conn = C::connect(info, Some(self.cluster_params.connection_timeout))?;
        if self.read_from_replicas {
            // If READONLY is sent to primary nodes, it will have no effect
            cmd("READONLY").query(&mut conn)?;
        }
        conn.set_read_timeout(*self.read_timeout.borrow())?;
        conn.set_write_timeout(*self.write_timeout.borrow())?;
        Ok(conn)
    }

    fn get_connection<'a>(
        &self,
        connections: &'a mut HashMap<String, C>,
        route: &Route,
    ) -> RedisResult<(String, &'a mut C)> {
        let slots = self.slots.borrow();
        if let Some(addr) = slots.slot_addr_for_route(route) {
            Ok((
                addr.to_string(),
                self.get_connection_by_addr(connections, addr)?,
            ))
        } else {
            // try a random node next.  This is safe if slots are involved
            // as a wrong node would reject the request.
            Ok(get_random_connection(connections))
        }
    }

    fn get_connection_by_addr<'a>(
        &self,
        connections: &'a mut HashMap<String, C>,
        addr: &str,
    ) -> RedisResult<&'a mut C> {
        if connections.contains_key(addr) {
            Ok(connections.get_mut(addr).unwrap())
        } else {
            // Create new connection.
            // TODO: error handling
            let conn = self.connect(addr)?;
            Ok(connections.entry(addr.to_string()).or_insert(conn))
        }
    }

    fn get_addr_for_cmd(&self, cmd: &Cmd) -> RedisResult<String> {
        let slots = self.slots.borrow();

        let addr_for_slot = |route: Route| -> RedisResult<String> {
            let slot_addr = slots
                .slot_addr_for_route(&route)
                .ok_or((ErrorKind::ClusterDown, "Missing slot coverage"))?;
            Ok(slot_addr.to_string())
        };

        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)) => {
                let mut rng = thread_rng();
                Ok(addr_for_slot(Route::new(
                    rng.gen_range(0..SLOT_SIZE),
                    SlotAddr::Master,
                ))?)
            }
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(route))) => {
                Ok(addr_for_slot(route)?)
            }
            _ => fail!(UNROUTABLE_ERROR),
        }
    }

    fn map_cmds_to_nodes(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut cmd_map: HashMap<String, NodeCmd> = HashMap::new();

        for (idx, cmd) in cmds.iter().enumerate() {
            let addr = self.get_addr_for_cmd(cmd)?;
            let nc = cmd_map
                .entry(addr.clone())
                .or_insert_with(|| NodeCmd::new(addr));
            nc.indexes.push(idx);
            cmd.write_packed_command(&mut nc.pipe);
        }

        let mut result = Vec::new();
        for (_, v) in cmd_map.drain() {
            result.push(v);
        }
        Ok(result)
    }

    fn execute_on_multiple_nodes<T, F>(
        &self,
        mut func: F,
        routing: MultipleNodeRoutingInfo,
    ) -> RedisResult<T>
    where
        T: MergeResults,
        F: FnMut(&mut C) -> RedisResult<T>,
    {
        let mut connections = self.connections.borrow_mut();
        let slots = self.slots.borrow_mut();
        let mut results = HashMap::new();

        // TODO: reconnect and shit
        let addresses = slots.addresses_for_multi_routing(&routing);
        for addr in addresses {
            let addr = addr.to_string();
            if let Some(connection) = connections.get_mut(&addr) {
                results.insert(addr, func(connection)?);
            }
        }

        Ok(T::merge_results(results))
    }

    #[allow(clippy::unnecessary_unwrap)]
    fn request<R, T, F>(&self, cmd: &R, mut func: F) -> RedisResult<T>
    where
        R: ?Sized + Routable,
        T: MergeResults + std::fmt::Debug,
        F: FnMut(&mut C) -> RedisResult<T>,
    {
        let route = match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)) => None,
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(route))) => {
                Some(route)
            }
            Some(RoutingInfo::MultiNode(multi_node_routing)) => {
                return self.execute_on_multiple_nodes(func, multi_node_routing);
            }
            None => fail!(UNROUTABLE_ERROR),
        };

        let mut retries = 0;
        let mut redirected = None::<Redirect>;

        loop {
            // Get target address and response.
            let (addr, rv) = {
                let mut connections = self.connections.borrow_mut();
                let (addr, conn) = if let Some(redirected) = redirected.take() {
                    let (addr, is_asking) = match redirected {
                        Redirect::Moved(addr) => (addr, false),
                        Redirect::Ask(addr) => (addr, true),
                    };
                    let conn = self.get_connection_by_addr(&mut connections, &addr)?;
                    if is_asking {
                        // if we are in asking mode we want to feed a single
                        // ASKING command into the connection before what we
                        // actually want to execute.
                        conn.req_packed_command(&b"*1\r\n$6\r\nASKING\r\n"[..])?;
                    }
                    (addr.to_string(), conn)
                } else if route.is_none() {
                    get_random_connection(&mut connections)
                } else {
                    self.get_connection(&mut connections, route.as_ref().unwrap())?
                };
                (addr, func(conn))
            };

            match rv {
                Ok(rv) => return Ok(rv),
                Err(err) => {
                    if retries == self.cluster_params.retry_params.number_of_retries {
                        return Err(err);
                    }
                    retries += 1;

                    match err.kind() {
                        ErrorKind::Ask => {
                            redirected = err
                                .redirect_node()
                                .map(|(node, _slot)| Redirect::Ask(node.to_string()));
                        }
                        ErrorKind::Moved => {
                            // Refresh slots.
                            self.refresh_slots()?;
                            // Request again.
                            redirected = err
                                .redirect_node()
                                .map(|(node, _slot)| Redirect::Moved(node.to_string()));
                        }
                        ErrorKind::TryAgain | ErrorKind::ClusterDown => {
                            // Sleep and retry.
                            let sleep_time = self
                                .cluster_params
                                .retry_params
                                .wait_time_for_retry(retries);
                            thread::sleep(sleep_time);
                        }
                        ErrorKind::IoError => {
                            if *self.auto_reconnect.borrow() {
                                if let Ok(mut conn) = self.connect(&addr) {
                                    if conn.check_connection() {
                                        self.connections.borrow_mut().insert(addr, conn);
                                    }
                                }
                            }
                        }
                        _ => {
                            if !err.is_retryable() {
                                return Err(err);
                            }
                        }
                    }
                }
            }
        }
    }

    fn send_recv_and_retry_cmds(&self, cmds: &[Cmd]) -> RedisResult<Vec<Value>> {
        // Vector to hold the results, pre-populated with `Nil` values. This allows the original
        // cmd ordering to be re-established by inserting the response directly into the result
        // vector (e.g., results[10] = response).
        let mut results = vec![Value::Nil; cmds.len()];

        let to_retry = self
            .send_all_commands(cmds)
            .and_then(|node_cmds| self.recv_all_commands(&mut results, &node_cmds))?;

        if to_retry.is_empty() {
            return Ok(results);
        }

        // Refresh the slots to ensure that we have a clean slate for the retry attempts.
        self.refresh_slots()?;

        // Given that there are commands that need to be retried, it means something in the cluster
        // topology changed. Execute each command seperately to take advantage of the existing
        // retry logic that handles these cases.
        for retry_idx in to_retry {
            let cmd = &cmds[retry_idx];
            results[retry_idx] = self.request(cmd, move |conn| conn.req_command(cmd))?;
        }
        Ok(results)
    }

    // Build up a pipeline per node, then send it
    fn send_all_commands(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut connections = self.connections.borrow_mut();

        let node_cmds = self.map_cmds_to_nodes(cmds)?;
        for nc in &node_cmds {
            self.get_connection_by_addr(&mut connections, &nc.addr)?
                .send_packed_command(&nc.pipe)?;
        }
        Ok(node_cmds)
    }

    // Receive from each node, keeping track of which commands need to be retried.
    fn recv_all_commands(
        &self,
        results: &mut [Value],
        node_cmds: &[NodeCmd],
    ) -> RedisResult<Vec<usize>> {
        let mut to_retry = Vec::new();
        let mut connections = self.connections.borrow_mut();
        let mut first_err = None;

        for nc in node_cmds {
            for cmd_idx in &nc.indexes {
                match self
                    .get_connection_by_addr(&mut connections, &nc.addr)?
                    .recv_response()
                {
                    Ok(item) => results[*cmd_idx] = item,
                    Err(err) if err.is_cluster_error() => to_retry.push(*cmd_idx),
                    Err(err) => first_err = first_err.or(Some(err)),
                }
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(to_retry),
        }
    }
}

impl<C: Connect + ConnectionLike> ConnectionLike for ClusterConnection<C> {
    fn supports_pipelining(&self) -> bool {
        false
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.request(cmd, move |conn| conn.req_command(cmd))
    }

    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        let value = parse_redis_value(cmd)?;
        self.request(&value, move |conn| conn.req_packed_command(cmd))
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let value = parse_redis_value(cmd)?;
        self.request(&value, move |conn| {
            conn.req_packed_commands(cmd, offset, count)
        })
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn is_open(&self) -> bool {
        let connections = self.connections.borrow();
        for conn in connections.values() {
            if !conn.is_open() {
                return false;
            }
        }
        true
    }

    fn check_connection(&mut self) -> bool {
        let mut connections = self.connections.borrow_mut();
        for conn in connections.values_mut() {
            if !conn.check_connection() {
                return false;
            }
        }
        true
    }
}

trait MergeResults {
    fn merge_results(_values: HashMap<String, Self>) -> Self
    where
        Self: Sized;
}

impl MergeResults for Value {
    fn merge_results(values: HashMap<String, Value>) -> Value {
        let mut items = vec![];
        for (addr, value) in values.into_iter() {
            items.push(Value::Bulk(vec![Value::Data(addr.into_bytes()), value]));
        }
        Value::Bulk(items)
    }
}

impl MergeResults for Vec<Value> {
    fn merge_results(_values: HashMap<String, Vec<Value>>) -> Vec<Value> {
        unreachable!("attempted to merge a pipeline. This should not happen");
    }
}

#[derive(Debug)]
struct NodeCmd {
    // The original command indexes
    indexes: Vec<usize>,
    pipe: Vec<u8>,
    addr: String,
}

impl NodeCmd {
    fn new(a: String) -> NodeCmd {
        NodeCmd {
            indexes: vec![],
            pipe: vec![],
            addr: a,
        }
    }
}

#[derive(Derivative)]
#[derivative(PartialEq, PartialOrd, Ord)]
#[derive(Debug, Eq)]
pub(crate) struct TopologyView {
    #[derivative(PartialOrd = "ignore", Ord = "ignore")]
    pub(crate) hash_value: u64,
    #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
    pub(crate) topology_value: Value,
    #[derivative(PartialEq = "ignore")]
    pub(crate) nodes_count: u16,
}

/// TlsMode indicates use or do not use verification of certification.
/// Check [ConnectionAddr](ConnectionAddr::TcpTls::insecure) for more.
#[derive(Clone, Copy)]
pub enum TlsMode {
    /// Secure verify certification.
    Secure,
    /// Insecure do not verify certification.
    Insecure,
}

// TODO: This function can panic and should probably
// return an Option instead:
fn get_random_connection<C: ConnectionLike + Connect + Sized>(
    connections: &mut HashMap<String, C>,
) -> (String, &mut C) {
    let addr = connections
        .keys()
        .choose(&mut thread_rng())
        .expect("Connections is empty")
        .to_string();
    let con = connections.get_mut(&addr).expect("Connections is empty");
    (addr, con)
}

// Parse slot data from raw redis value.
pub(crate) fn parse_slots(raw_slot_resp: &Value, tls: Option<TlsMode>) -> RedisResult<Vec<Slot>> {
    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = raw_slot_resp {
        let mut iter = items.iter();
        while let Some(Value::Bulk(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };
                        if ip.is_empty() {
                            return None;
                        }

                        let port = if let Value::Int(port) = node[1] {
                            port as u16
                        } else {
                            return None;
                        };
                        Some(get_connection_addr(ip.into_owned(), port, tls).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot::new(start, end, nodes.pop().unwrap(), replicas));
        }
    }

    Ok(result)
}

pub(crate) fn build_slot_map(
    slot_map: &mut SlotMap,
    mut slots_data: Vec<Slot>,
    read_from_replicas: bool,
) -> RedisResult<()> {
    slots_data.sort_by_key(|slot_data| slot_data.start());
    let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
        if prev_end != slot_data.start() {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!(
                    "Received overlapping slots {} and {}..{}",
                    prev_end,
                    slot_data.start(),
                    slot_data.end()
                ),
            )));
        }
        Ok(slot_data.end() + 1)
    })?;

    if last_slot != SLOT_SIZE {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error.",
            format!("Lacks the slots >= {last_slot}"),
        )));
    }
    slot_map.clear();
    slot_map.fill_slots(&slots_data, read_from_replicas);
    trace!("{:?}", slot_map);
    Ok(())
}

// The node string passed to this function will always be in the format host:port as it is either:
// - Created by calling ConnectionAddr::to_string (unix connections are not supported in cluster mode)
// - Returned from redis via the ASK/MOVED response
pub(crate) fn get_connection_info(
    node: &str,
    cluster_params: ClusterParams,
) -> RedisResult<ConnectionInfo> {
    let invalid_error = || (ErrorKind::InvalidClientConfig, "Invalid node string");

    let (host, port) = node
        .rsplit_once(':')
        .and_then(|(host, port)| {
            Some(host.trim_start_matches('[').trim_end_matches(']'))
                .filter(|h| !h.is_empty())
                .zip(u16::from_str(port).ok())
        })
        .ok_or_else(invalid_error)?;

    Ok(ConnectionInfo {
        addr: get_connection_addr(host.to_string(), port, cluster_params.tls),
        redis: RedisConnectionInfo {
            password: cluster_params.password,
            username: cluster_params.username,
            ..Default::default()
        },
    })
}

fn get_connection_addr(host: String, port: u16, tls: Option<TlsMode>) -> ConnectionAddr {
    match tls {
        Some(TlsMode::Secure) => ConnectionAddr::TcpTls {
            host,
            port,
            insecure: false,
        },
        Some(TlsMode::Insecure) => ConnectionAddr::TcpTls {
            host,
            port,
            insecure: true,
        },
        _ => ConnectionAddr::Tcp(host, port),
    }
}

pub(crate) fn slot_cmd() -> Cmd {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    cmd
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn calculate_topology(
    topology_views: Vec<Value>,
    retries: Option<Arc<atomic::AtomicUsize>>, // TODO: change to usize
    tls_mode: Option<TlsMode>,
    read_from_replicas: bool,
    num_of_queried_nodes: usize,
) -> Result<SlotMap, RedisError> {
    if topology_views.is_empty() {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: All CLUSTER SLOTS results are errors",
        )));
    }
    const MIN_ACCURACY_RATE: f32 = 0.2;
    let mut hash_view_map = HashMap::new();
    let mut new_slots = SlotMap::new();
    for view in topology_views {
        let hash_value = calculate_hash(&view);
        let topology_entry = hash_view_map.entry(hash_value).or_insert(TopologyView {
            hash_value,
            topology_value: view,
            nodes_count: 0,
        });
        topology_entry.nodes_count += 1;
    }
    let mut most_frequent_topology: Option<&TopologyView> = None;
    let mut has_more_than_a_single_max = false;
    let vec_iter = hash_view_map.iter().map(|(_, view)| view);
    // Find the most frequent topology view
    for curr_view in vec_iter {
        let max_view = match most_frequent_topology {
            Some(view) => view,
            None => {
                most_frequent_topology = Some(curr_view);
                continue;
            }
        };
        match max_view.cmp(curr_view) {
            std::cmp::Ordering::Less => {
                most_frequent_topology = Some(curr_view);
                has_more_than_a_single_max = false;
            }
            std::cmp::Ordering::Equal => has_more_than_a_single_max = true,
            std::cmp::Ordering::Greater => continue,
        }
    }
    let most_frequent_topology = match most_frequent_topology {
        Some(view) => view,
        None => unreachable!(),
    };
    if has_more_than_a_single_max {
        // More than a single most frequent view was found
        if (retries.is_some() && retries.unwrap().fetch_sub(1, atomic::Ordering::SeqCst) == 1)
            || num_of_queried_nodes < 3
        {
            // If it's the last retry, or if we it's a 2-nodes cluster, we'll return all found topologies to be checked by the caller
            for (idx, topology_view) in hash_view_map.iter() {
                match parse_slots(&topology_view.topology_value, tls_mode)
                    .and_then(|v| build_slot_map(&mut new_slots, v, read_from_replicas))
                {
                    Ok(_) => {
                        return Ok(new_slots);
                    }
                    Err(e) => {
                        // If it's the last view, raise the error
                        if *idx as usize == hash_view_map.len() - 1 {
                            return Err(e);
                        } else {
                            continue;
                        }
                    }
                }
            }
        }
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: Couldn't get a majority in topology views",
        )));
    }
    // Calculates the accuracy of the topology view by checking how many nodes share this view out of the total number queried
    let accuracy_rate = most_frequent_topology.nodes_count as f32 / num_of_queried_nodes as f32;
    if accuracy_rate >= MIN_ACCURACY_RATE {
        parse_slots(&most_frequent_topology.topology_value, tls_mode)
            .and_then(|v| build_slot_map(&mut new_slots, v, read_from_replicas))?;
        Ok(new_slots)
    } else {
        Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: The accuracy of the topology view is too low",
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use crate::cluster_routing::SlotAddrs;

    use super::*;

    #[test]
    fn parse_cluster_node_host_port() {
        let cases = vec![
            (
                "127.0.0.1:6379",
                ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379u16),
            ),
            (
                "localhost.localdomain:6379",
                ConnectionAddr::Tcp("localhost.localdomain".to_string(), 6379u16),
            ),
            (
                "dead::cafe:beef:30001",
                ConnectionAddr::Tcp("dead::cafe:beef".to_string(), 30001u16),
            ),
            (
                "[fe80::cafe:beef%en1]:30001",
                ConnectionAddr::Tcp("fe80::cafe:beef%en1".to_string(), 30001u16),
            ),
        ];

        for (input, expected) in cases {
            let res = get_connection_info(input, ClusterParams::default());
            assert_eq!(res.unwrap().addr, expected);
        }

        let cases = vec![":0", "[]:6379"];
        for input in cases {
            let res = get_connection_info(input, ClusterParams::default());
            assert_eq!(
                res.err(),
                Some(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Invalid node string",
                ))),
            );
        }
    }

    #[test]
    fn test_topology_calculator() {
        let single_node_view = Value::Bulk(vec![Value::Bulk(vec![
            Value::Int(0_i64),
            Value::Int(16383_i64),
            Value::Bulk(vec![
                Value::Data("node1".as_bytes().to_vec()),
                Value::Int(6379_i64),
            ]),
        ])]);
        let single_node_missing_slots_view = Value::Bulk(vec![Value::Bulk(vec![
            Value::Int(0_i64),
            Value::Int(4000_i64),
            Value::Bulk(vec![
                Value::Data("node1".as_bytes().to_vec()),
                Value::Int(6379_i64),
            ]),
        ])]);
        let two_nodes_full_coverage_view = Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Int(0_i64),
                Value::Int(4000_i64),
                Value::Bulk(vec![
                    Value::Data("node1".as_bytes().to_vec()),
                    Value::Int(6379_i64),
                ]),
            ]),
            Value::Bulk(vec![
                Value::Int(4001_i64),
                Value::Int(16383_i64),
                Value::Bulk(vec![
                    Value::Data("node2".as_bytes().to_vec()),
                    Value::Int(6380_i64),
                ]),
            ]),
        ]);
        let two_nodes_missing_slots_view = Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Int(0_i64),
                Value::Int(3000_i64),
                Value::Bulk(vec![
                    Value::Data("node3".as_bytes().to_vec()),
                    Value::Int(6381_i64),
                ]),
            ]),
            Value::Bulk(vec![
                Value::Int(4001_i64),
                Value::Int(16383_i64),
                Value::Bulk(vec![
                    Value::Data("node4".as_bytes().to_vec()),
                    Value::Int(6382_i64),
                ]),
            ]),
        ]);

        // 4 nodes queried (1 error): Has a majority, single_node_view should be chosen
        let mut queried_nodes: usize = 4;
        let topology_results = vec![
            single_node_view.clone(),
            single_node_view.clone(),
            two_nodes_full_coverage_view.clone(),
        ];
        let node1_addr = SlotAddrs::new("node1:6379".to_string(), None);
        let node2_addr = SlotAddrs::new("node2:6380".to_string(), None);
        let topology_view =
            calculate_topology(topology_results, None, None, false, queried_nodes).unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let excepted = vec![&node1_addr];
        assert_eq!(res, excepted);

        // 3 nodes queried: No majority, should return an error
        queried_nodes = 3;
        let topology_results = vec![
            single_node_view,
            two_nodes_full_coverage_view.clone(),
            two_nodes_missing_slots_view.clone(),
        ];
        let topology_view = calculate_topology(topology_results, None, None, false, queried_nodes);
        assert!(topology_view.is_err());

        // 3 nodes queried:: No majority, last retry, should get the view that has a full slot coverage
        let topology_results = vec![
            single_node_missing_slots_view,
            two_nodes_full_coverage_view.clone(),
            two_nodes_missing_slots_view.clone(),
        ];
        let topology_view = calculate_topology(
            topology_results,
            Some(Arc::new(AtomicUsize::new(1))),
            None,
            false,
            queried_nodes,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let excepted: Vec<&SlotAddrs> = vec![&node1_addr, &node2_addr];
        assert_eq!(res, excepted);

        //  2 nodes queried: No majority, should get the view that has a full slot coverage
        queried_nodes = 2;
        let topology_results = vec![two_nodes_full_coverage_view, two_nodes_missing_slots_view];
        let topology_view =
            calculate_topology(topology_results, None, None, false, queried_nodes).unwrap();
        let res: Vec<_> = topology_view.values().collect();
        assert_eq!(res, excepted);
    }
}
