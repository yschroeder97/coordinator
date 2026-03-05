use crate::worker::CreateWorker;
use crate::worker::arb_capacity;
use crate::worker::arb_create_worker;
use crate::worker::endpoint::{HostAddr, NetworkAddr, DEFAULT_DATA_PORT, DEFAULT_GRPC_PORT};
use proptest::prelude::*;

fn arb_n_unique_workers(n: usize) -> impl Strategy<Value = Vec<CreateWorker>> {
    prop::collection::vec(arb_create_worker(), n..=n).prop_map(|workers| {
        workers
            .into_iter()
            .enumerate()
            .map(|(i, mut w)| {
                w.host_addr.port = 10000 + i as u16;
                w.grpc_addr.port = 20000 + i as u16;
                w
            })
            .collect()
    })
}

const MAX_SIM_WORKERS: u8 = 32;

pub fn arb_topology(min_workers: u8) -> impl Strategy<Value = Vec<CreateWorker>> {
    (min_workers..=MAX_SIM_WORKERS).prop_flat_map(|n| {
        prop::collection::vec(arb_capacity(), n as usize..=n as usize).prop_map(|capacities| {
            capacities
                .into_iter()
                .enumerate()
                .map(|(i, capacity)| {
                    let octet = (i + 1) as u8;
                    let host = NetworkAddr::new(format!("192.168.2.{octet}"), DEFAULT_DATA_PORT);
                    let grpc = NetworkAddr::new(format!("192.168.2.{octet}"), DEFAULT_GRPC_PORT);
                    CreateWorker::new(host, grpc, capacity)
                })
                .collect()
        })
    })
}

pub fn arb_dag_topology(max_workers: usize) -> impl Strategy<Value = Vec<CreateWorker>> {
    (2..=max_workers).prop_flat_map(|n| {
        let max_edges = n * (n - 1) / 2;
        (
            arb_n_unique_workers(n),
            prop::collection::vec(any::<bool>(), max_edges..=max_edges),
        )
            .prop_map(|(mut workers, flags)| {
                let mut peers_per_worker: Vec<Vec<HostAddr>> =
                    vec![Vec::new(); workers.len()];
                let mut idx = 0;
                for i in 0..workers.len() {
                    for j in (i + 1)..workers.len() {
                        if flags[idx] {
                            peers_per_worker[i].push(workers[j].host_addr.clone());
                        }
                        idx += 1;
                    }
                }
                for (i, peers) in peers_per_worker.into_iter().enumerate() {
                    workers[i].peers = peers;
                }
                workers
            })
    })
}
