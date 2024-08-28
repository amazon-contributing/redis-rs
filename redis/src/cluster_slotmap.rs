use std::sync::Arc;
use std::sync::RwLock;
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Display,
    sync::atomic::AtomicUsize,
};

use dashmap::DashMap;

use crate::cluster_routing::{Route, ShardAddrs, Slot, SlotAddr};

pub(crate) type NodesMap = DashMap<Arc<String>, Arc<RwLock<ShardAddrs>>>;

#[derive(Debug)]
pub(crate) struct SlotMapValue {
    pub(crate) start: u16,
    pub(crate) addrs: Arc<RwLock<ShardAddrs>>,
    pub(crate) last_used_replica: AtomicUsize,
}

#[derive(Debug, Default, Clone, PartialEq, Copy)]
pub(crate) enum ReadFromReplicaStrategy {
    #[default]
    AlwaysFromPrimary,
    RoundRobin,
}

#[derive(Debug, Default)]
pub(crate) struct SlotMap {
    pub(crate) slots: BTreeMap<u16, SlotMapValue>,
    nodes_map: NodesMap,
    read_from_replica: ReadFromReplicaStrategy,
}

fn get_address_from_slot(
    slot: &SlotMapValue,
    read_from_replica: ReadFromReplicaStrategy,
    slot_addr: SlotAddr,
) -> Arc<String> {
    let addrs = slot
        .addrs
        .read()
        .expect("Failed to obtain ShardAddrs's read lock");
    if slot_addr == SlotAddr::Master || addrs.replicas().is_empty() {
        return addrs.primary();
    }
    match read_from_replica {
        ReadFromReplicaStrategy::AlwaysFromPrimary => addrs.primary(),
        ReadFromReplicaStrategy::RoundRobin => {
            let index = slot
                .last_used_replica
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % addrs.replicas().len();
            addrs.replicas()[index].clone()
        }
    }
}

impl SlotMap {
    pub(crate) fn new_with_read_strategy(read_from_replica: ReadFromReplicaStrategy) -> Self {
        SlotMap {
            slots: BTreeMap::new(),
            nodes_map: DashMap::new(),
            read_from_replica,
        }
    }

    pub(crate) fn new(slots: Vec<Slot>, read_from_replica: ReadFromReplicaStrategy) -> Self {
        let mut slot_map = SlotMap::new_with_read_strategy(read_from_replica);
        let mut shard_id = 0;
        for slot in slots {
            let primary = Arc::new(slot.master);
            // Get the shard addresses if the primary is already in nodes_map;
            // otherwise, create a new ShardAddrs and add it
            let shard_addrs_arc = slot_map
                .nodes_map
                .entry(primary.clone())
                .or_insert_with(|| {
                    shard_id += 1;
                    let replicas: Vec<Arc<String>> =
                        slot.replicas.into_iter().map(Arc::new).collect();
                    Arc::new(RwLock::new(ShardAddrs::new(primary, replicas)))
                })
                .clone();

            let replicas_reader = shard_addrs_arc
                .read()
                .expect("Failed to obtain reader lock for ShardAddrs");
            // Add all replicas to nodes_map with a reference to the same ShardAddrs if not already present
            replicas_reader.replicas().iter().for_each(|replica| {
                slot_map
                    .nodes_map
                    .entry(replica.clone())
                    .or_insert(shard_addrs_arc.clone());
            });

            // Insert the slot value into the slots map
            slot_map.slots.insert(
                slot.end,
                SlotMapValue {
                    addrs: shard_addrs_arc.clone(),
                    start: slot.start,
                    last_used_replica: AtomicUsize::new(0),
                },
            );
        }

        slot_map
    }

    #[allow(dead_code)] // used in tests
    pub(crate) fn nodes_map(&self) -> &NodesMap {
        &self.nodes_map
    }

    pub fn is_primary(&self, address: &String) -> bool {
        self.nodes_map.get(address).map_or(false, |shard_addrs| {
            *shard_addrs
                .read()
                .expect("Failed to obtain ShardAddrs's read lock")
                .primary()
                == *address
        })
    }

    pub fn slot_value_for_route(&self, route: &Route) -> Option<&SlotMapValue> {
        let slot = route.slot();
        self.slots
            .range(slot..)
            .next()
            .and_then(|(end, slot_value)| {
                if slot <= *end && slot_value.start <= slot {
                    Some(slot_value)
                } else {
                    None
                }
            })
    }

    pub fn slot_addr_for_route(&self, route: &Route) -> Option<Arc<String>> {
        self.slot_value_for_route(route).map(|slot_value| {
            get_address_from_slot(slot_value, self.read_from_replica, route.slot_addr())
        })
    }

    pub fn addresses_for_all_primaries(&self) -> HashSet<Arc<String>> {
        self.nodes_map
            .iter()
            .map(|map_item| {
                let shard_addrs = map_item.value();
                shard_addrs
                    .read()
                    .expect("Failed to obtain ShardAddrs's read lock")
                    .primary()
                    .clone()
            })
            .collect()
    }

    pub fn all_node_addresses(&self) -> HashSet<Arc<String>> {
        self.nodes_map
            .iter()
            .map(|map_item| {
                let node_addr = map_item.key();
                node_addr.clone()
            })
            .collect()
    }

    pub fn addresses_for_multi_slot<'a, 'b>(
        &'a self,
        routes: &'b [(Route, Vec<usize>)],
    ) -> impl Iterator<Item = Option<Arc<String>>> + 'a
    where
        'b: 'a,
    {
        routes
            .iter()
            .map(|(route, _)| self.slot_addr_for_route(route))
    }

    // Returns the slots that are assigned to the given address.
    pub(crate) fn get_slots_of_node(&self, node_address: Arc<String>) -> Vec<u16> {
        self.slots
            .iter()
            .filter_map(|(end, slot_value)| {
                let addr_reader = slot_value
                    .addrs
                    .read()
                    .expect("Failed to obtain ShardAddrs's read lock");
                if addr_reader.primary() == node_address
                    || addr_reader.replicas().contains(&node_address)
                {
                    Some(slot_value.start..(*end + 1))
                } else {
                    None
                }
            })
            .flatten()
            .collect()
    }

    pub(crate) fn get_node_address_for_slot(
        &self,
        slot: u16,
        slot_addr: SlotAddr,
    ) -> Option<Arc<String>> {
        self.slots.range(slot..).next().and_then(|(_, slot_value)| {
            if slot_value.start <= slot {
                Some(get_address_from_slot(
                    slot_value,
                    self.read_from_replica,
                    slot_addr,
                ))
            } else {
                None
            }
        })
    }
}

impl Display for SlotMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Strategy: {:?}. Slot mapping:", self.read_from_replica)?;
        for (end, slot_map_value) in self.slots.iter() {
            let shard_addrs = slot_map_value
                .addrs
                .read()
                .expect("Failed to obtain ShardAddrs's read lock");
            writeln!(
                f,
                "({}-{}): primary: {}, replicas: {:?}",
                slot_map_value.start,
                end,
                shard_addrs.primary(),
                shard_addrs.replicas()
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests_cluster_slotmap {
    use super::*;

    fn process_expected(expected: Vec<&str>) -> HashSet<Arc<String>> {
        <HashSet<&str> as IntoIterator>::into_iter(HashSet::from_iter(expected))
            .map(|s| Arc::new(s.to_string()))
            .collect()
    }

    fn process_expected_with_option(expected: Vec<Option<&str>>) -> Vec<Arc<String>> {
        expected
            .into_iter()
            .filter_map(|opt| opt.map(|s| Arc::new(s.to_string())))
            .collect()
    }

    #[test]
    fn test_slot_map_retrieve_routes() {
        let slot_map = SlotMap::new(
            vec![
                Slot::new(
                    1,
                    1000,
                    "node1:6379".to_owned(),
                    vec!["replica1:6379".to_owned()],
                ),
                Slot::new(
                    1002,
                    2000,
                    "node2:6379".to_owned(),
                    vec!["replica2:6379".to_owned()],
                ),
            ],
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        );

        assert!(slot_map
            .slot_addr_for_route(&Route::new(0, SlotAddr::Master))
            .is_none());
        assert_eq!(
            "node1:6379",
            *slot_map
                .slot_addr_for_route(&Route::new(1, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            *slot_map
                .slot_addr_for_route(&Route::new(500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            *slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::Master))
                .unwrap()
        );
        assert!(slot_map
            .slot_addr_for_route(&Route::new(1001, SlotAddr::Master))
            .is_none());

        assert_eq!(
            "node2:6379",
            *slot_map
                .slot_addr_for_route(&Route::new(1002, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            *slot_map
                .slot_addr_for_route(&Route::new(1500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            *slot_map
                .slot_addr_for_route(&Route::new(2000, SlotAddr::Master))
                .unwrap()
        );
        assert!(slot_map
            .slot_addr_for_route(&Route::new(2001, SlotAddr::Master))
            .is_none());
    }

    fn get_slot_map(read_from_replica: ReadFromReplicaStrategy) -> SlotMap {
        SlotMap::new(
            vec![
                Slot::new(
                    1,
                    1000,
                    "node1:6379".to_owned(),
                    vec!["replica1:6379".to_owned()],
                ),
                Slot::new(
                    1002,
                    2000,
                    "node2:6379".to_owned(),
                    vec!["replica2:6379".to_owned(), "replica3:6379".to_owned()],
                ),
                Slot::new(
                    2001,
                    3000,
                    "node3:6379".to_owned(),
                    vec![
                        "replica4:6379".to_owned(),
                        "replica5:6379".to_owned(),
                        "replica6:6379".to_owned(),
                    ],
                ),
                Slot::new(
                    3001,
                    4000,
                    "node2:6379".to_owned(),
                    vec!["replica2:6379".to_owned(), "replica3:6379".to_owned()],
                ),
            ],
            read_from_replica,
        )
    }

    #[test]
    fn test_slot_map_get_all_primaries() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::AlwaysFromPrimary);
        let addresses = slot_map.addresses_for_all_primaries();
        assert_eq!(
            addresses,
            process_expected(vec!["node1:6379", "node2:6379", "node3:6379"])
        );
    }

    #[test]
    fn test_slot_map_get_all_nodes() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::AlwaysFromPrimary);
        let addresses = slot_map.all_node_addresses();
        assert_eq!(
            addresses,
            process_expected(vec![
                "node1:6379",
                "node2:6379",
                "node3:6379",
                "replica1:6379",
                "replica2:6379",
                "replica3:6379",
                "replica4:6379",
                "replica5:6379",
                "replica6:6379"
            ])
        );
    }

    #[test]
    fn test_slot_map_get_multi_node() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes)
            .collect::<Vec<_>>();
        assert!(addresses.contains(&Some(Arc::new("node1:6379".to_string()))));
        assert!(
            addresses.contains(&Some(Arc::new("replica4:6379".to_string())))
                || addresses.contains(&Some(Arc::new("replica5:6379".to_string())))
                || addresses.contains(&Some(Arc::new("replica6:6379".to_string())))
        );
    }

    /// This test is needed in order to verify that if the MultiSlot route finds the same node for more than a single route,
    /// that node's address will appear multiple times, in the same order.
    #[test]
    fn test_slot_map_get_repeating_addresses_when_the_same_node_is_found_in_multi_slot() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
        let routes = vec![
            (Route::new(1, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2001, SlotAddr::Master), vec![]),
            (Route::new(2, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2002, SlotAddr::Master), vec![]),
            (Route::new(3, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2003, SlotAddr::Master), vec![]),
        ];
        let addresses: Vec<Arc<String>> = slot_map
            .addresses_for_multi_slot(&routes)
            .flatten()
            .collect();

        assert_eq!(
            addresses,
            process_expected_with_option(vec![
                Some("replica1:6379"),
                Some("node3:6379"),
                Some("replica1:6379"),
                Some("node3:6379"),
                Some("replica1:6379"),
                Some("node3:6379")
            ])
        );
    }

    #[test]
    fn test_slot_map_get_none_when_slot_is_missing_from_multi_slot() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
        let routes = vec![
            (Route::new(1, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(5000, SlotAddr::Master), vec![]),
            (Route::new(6000, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2002, SlotAddr::Master), vec![]),
        ];
        let addresses: Vec<Arc<String>> = slot_map
            .addresses_for_multi_slot(&routes)
            .flatten()
            .collect();

        assert_eq!(
            addresses,
            process_expected_with_option(vec![
                Some("replica1:6379"),
                None,
                None,
                Some("node3:6379")
            ])
        );
    }

    #[test]
    fn test_slot_map_rotate_read_replicas() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
        let route = Route::new(2001, SlotAddr::ReplicaOptional);
        let mut addresses = vec![
            slot_map.slot_addr_for_route(&route).unwrap(),
            slot_map.slot_addr_for_route(&route).unwrap(),
            slot_map.slot_addr_for_route(&route).unwrap(),
        ];
        addresses.sort();
        assert_eq!(
            addresses,
            vec!["replica4:6379", "replica5:6379", "replica6:6379"]
                .into_iter()
                .map(|s| Arc::new(s.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_get_slots_of_node() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::AlwaysFromPrimary);
        assert_eq!(
            slot_map.get_slots_of_node(Arc::new("node1:6379".to_string())),
            (1..1001).collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node(Arc::new("node2:6379".to_string())),
            vec![1002..2001, 3001..4001]
                .into_iter()
                .flatten()
                .collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node(Arc::new("replica3:6379".to_string())),
            vec![1002..2001, 3001..4001]
                .into_iter()
                .flatten()
                .collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node(Arc::new("replica4:6379".to_string())),
            (2001..3001).collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node(Arc::new("replica5:6379".to_string())),
            (2001..3001).collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node(Arc::new("replica6:6379".to_string())),
            (2001..3001).collect::<Vec<u16>>()
        );
    }
}
