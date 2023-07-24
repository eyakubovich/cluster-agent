pub mod config;
pub mod kube_state;
pub mod platform;
pub mod version;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use log::*;
use prost_types::Timestamp;
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;

use config::Config;
use kube_state::{Container, KubeResource, KubeState, Machine};
use platform::pb;

use crate::kube_state::KubeEvent;

const RECONNECT_INTERVAL: Duration = Duration::from_secs(15);
const NODES_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

const TIMESTAMP_INFINITY: Timestamp = Timestamp {
    seconds: 4134009600, // 2101-01-01
    nanos: 0,
};

const LABEL_NODE_NAME: &str = "kube:node:name";
const LABEL_NS_NAME: &str = "kube:namespace:name";

#[derive(Parser)]
struct CliArgs {
    #[clap(long = "config", default_value = config::CONFIG_PATH)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let config = Config::load(args.config, None)?;

    std::env::set_var("RUST_LOG", config.log_level());
    pretty_env_logger::init();

    info!("EdgeBit Cluster Agent v{}", version::VERSION);

    let (kube, mut events) = match KubeState::with_defaults().await {
        Ok(kube) => kube,
        Err(err) => return Err(anyhow!("Could not connect to kube api-server: {err}")),
    };

    let kube = Arc::new(kube);

    kube.load_all().await?;

    loop {
        if let Err(err) = run(&config, kube.clone(), &mut events).await {
            error!("{err}");
        }

        tokio::time::sleep(RECONNECT_INTERVAL).await;
    }
}

async fn run(
    config: &Config,
    kube: Arc<KubeState>,
    events: &mut Receiver<KubeEvent>,
) -> Result<()> {
    let url = config.edgebit_url();
    let token = config.edgebit_id();

    info!("Connecting to EdgeBit at {url}");

    let mut edgebit = platform::Client::connect(url.try_into()?, token).await?;

    insert_all(&mut edgebit, kube.clone(), config.cluster_name()).await?;

    {
        let kube = kube.clone();
        tokio::task::spawn(async move {
            _ = kube.watch_all().await;
        });
    }

    let mut nodes_heartbeats = tokio::time::interval_at(
        Instant::now() + NODES_HEARTBEAT_INTERVAL,
        NODES_HEARTBEAT_INTERVAL,
    );

    loop {
        tokio::select! {
            Some(event) = events.recv() => {
                match event {
                    KubeEvent::ContainersUpdated(containers) => {
                        handle_containers_updated(&mut edgebit, &kube, containers).await?;
                    }
                    KubeEvent::MachinesAdded(machines) => {
                        handle_machines_added(&mut edgebit, machines).await?;
                    }
                }
            },
            _ = nodes_heartbeats.tick() => {
                insert_nodes(&mut edgebit, &kube).await?;
            },
            else => break,
        }
    }

    Ok(())
}

async fn insert_all(
    edgebit: &mut platform::Client,
    kube: Arc<KubeState>,
    cluster_name: String,
) -> Result<()> {
    edgebit
        .upsert_cluster(pb::Cluster {
            kind: pb::ClusterKind::Kube as i32,
            id: kube.cluster_id().to_string(),
            name: cluster_name,
        })
        .await?;

    insert_nodes(edgebit, &kube).await?;

    let workloads: Vec<_> = kube
        .containers()
        .into_iter()
        .map(|c| container_into_pb(&kube, c))
        .collect();

    edgebit
        .reset_workloads(pb::ResetWorkloadsRequest {
            cluster_id: kube.cluster_id().to_string(),
            workloads,
        })
        .await?;

    Ok(())
}

fn container_into_pb(kube: &KubeState, c: Container) -> pb::WorkloadInstance {
    let machine_id = match kube.machine_id(&c.node_name) {
        Some(id) => id,
        None => {
            warn!("machine_id not found for node with name={}", c.node_name);
            String::new()
        }
    };

    if let Some(pod) = kube.get(kube_state::KIND_POD, &c.pod_uid) {
        let mut labels = HashMap::new();
        expand_labels(kube, &mut labels, kube_state::KIND_POD, &pod);

        labels.insert(LABEL_NS_NAME.to_string(), pod.namespace);

        pb::WorkloadInstance {
            workload_id: clean_workload_id(&c.container_id).to_string(),
            workload: Some(pb::Workload {
                labels,
                kind: Some(pb::workload::Kind::Container(pb::Container {
                    name: c.name,
                })),
            }),
            start_time: c.started_at.map(dt_to_timestamp),
            end_time: match c.finished_at {
                Some(dt) => Some(dt_to_timestamp(dt)),
                None => Some(TIMESTAMP_INFINITY),
            },
            image_id: String::new(), //clean_image_id(&c.image_id).to_string(),
            image: Some(pb::Image {
                kind: Some(pb::image::Kind::Docker(pb::DockerImage { tag: c.image })),
            }),
            machine_id,
        }
    } else {
        // the short version for cases where it's a deletion
        pb::WorkloadInstance {
            workload_id: clean_workload_id(&c.container_id).to_string(),
            workload: None,
            start_time: c.started_at.map(dt_to_timestamp),
            end_time: match c.finished_at {
                Some(dt) => Some(dt_to_timestamp(dt)),
                None => Some(TIMESTAMP_INFINITY),
            },
            image_id: String::new(), //clean_image_id(&c.image_id).to_string(),
            image: None,
            machine_id,
        }
    }
}

fn expand_labels(
    kube: &KubeState,
    labels: &mut HashMap<String, String>,
    kind: &str,
    resource: &KubeResource,
) {
    let name_key = match kube_state::kind_alias(kind) {
        Some(alias) => format!("kube:{}:name", alias),
        None => format!("kube:{}:name", kind.to_lowercase()),
    };

    labels.insert(name_key, resource.name.clone());

    labels.extend(
        resource
            .labels
            .iter()
            .map(|(k, v)| (namespaced_label_key(kind, k), v.clone())),
    );

    for owner_ref in &resource.owner_refs {
        if let Some(owner) = kube.get(&owner_ref.kind, &owner_ref.uid) {
            expand_labels(kube, labels, &owner_ref.kind, &owner);
        }
    }
}

fn namespaced_label_key(kind: &str, key: &str) -> String {
    match kube_state::kind_alias(kind) {
        Some(alias) => format!("kube:{alias}:labels:{key}"),
        None => format!("kube:{}:labels:{key}", kind.to_lowercase()),
    }
}

fn dt_to_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: 0,
    }
}

async fn handle_containers_updated(
    edgebit: &mut platform::Client,
    kube: &KubeState,
    containers: Vec<Container>,
) -> Result<()> {
    let workloads: Vec<_> = containers
        .into_iter()
        .map(|c| container_into_pb(kube, c))
        .collect();

    if !workloads.is_empty() {
        let req = pb::UpsertWorkloadsRequest {
            cluster_id: kube.cluster_id(),
            workloads,
        };

        edgebit.upsert_workloads(req).await?;
    }

    Ok(())
}

async fn handle_machines_added(
    edgebit: &mut platform::Client,
    machines: Vec<Machine>,
) -> Result<()> {
    if !machines.is_empty() {
        let req = pb::UpsertMachinesRequest {
            machines: machines
                .into_iter()
                .map(|m| pb::Machine {
                    labels: [(LABEL_NODE_NAME.to_string(), m.name)].into(),
                    id: m.id,
                    ..Default::default()
                })
                .collect(),
        };

        edgebit.upsert_machines(req).await?;
    }

    Ok(())
}

async fn insert_nodes(edgebit: &mut platform::Client, kube: &KubeState) -> Result<()> {
    let machines = kube
        .machines()
        .into_iter()
        .map(|m| pb::Machine {
            id: m.id,
            labels: [(LABEL_NODE_NAME.to_string(), m.name)].into(),
            ..Default::default()
        })
        .collect();

    edgebit
        .upsert_machines(pb::UpsertMachinesRequest { machines })
        .await?;

    Ok(())
}

fn clean_workload_id(id: &str) -> &str {
    // strip off the scheme: docker://432f545c6ba13b...
    match id.split_once("//") {
        Some((_, result)) => result,
        None => id,
    }
}

fn clean_image_id(id: &str) -> &str {
    // strip off the scheme + tag: docker-pullable://debian@432f545c6ba13b...
    match id.split_once('@') {
        Some((_, result)) => result,
        None => match id.split_once("//") {
            Some((_, result)) => result,
            None => id,
        },
    }
}
