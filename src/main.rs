pub mod config;
pub mod kube_state;
pub mod platform;
pub mod version;

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use std::path::PathBuf;

use log::*;
use clap::Parser;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost_types::Timestamp;

use config::Config;
use kube_state::{KubeState, KubeResource, Container};
use platform::pb;

const COLLECTION_INTERVAL: Duration = Duration::from_secs(600);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(60);

const TIMESTAMP_INFINITY: Timestamp = Timestamp {
    seconds: 4134009600, // 2101-01-01
    nanos: 0,
};

#[derive(Parser)]
struct CliArgs {
    #[clap(long = "config", default_value = config::CONFIG_PATH)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let args = CliArgs::parse();

    info!("EdgeBit Cluster Agent v{}", version::VERSION);

    let config = Config::load(args.config, None)?;

    let url = config.edgebit_url();
    let token = config.edgebit_id();

    info!("Connecting to EdgeBit at {url}");
    let edgebit = platform::Client::connect(
        url.try_into()?,
        token.try_into()?,
        config.hostname(),
    ).await?;

    run(edgebit).await;

    Ok(())
}

async fn run(mut edgebit: platform::Client) {
    loop {
        match collect(&mut edgebit).await {
            Ok(_) => {
                tokio::time::sleep(COLLECTION_INTERVAL).await;
            },
            Err(err) => {
                error!("{err}");
                tokio::time::sleep(RECONNECT_INTERVAL).await;
            }
        }
    }
}

async fn collect(edgebit: &mut platform::Client) -> Result<()> {
    let mut kube = match KubeState::with_defaults().await {
        Ok(kube) => kube,
        Err(err) => return Err(anyhow!("Could not connect to kube api-server: {err}")),
    };

    kube.load_all().await?;

    edgebit.upsert_cluster(kube.cluster_id().to_string()).await?;

    let machines = kube.machines()
        .into_iter()
        .map(|m| pb::Machine{
            id: m.id,
            hostname: m.name,
        })
        .collect();

    debug!("{machines:?}");

    edgebit.upsert_machines(machines).await?;

    let workloads = make_workloads(&kube)
        .into_iter()
        .map(|wl| wl.into_pb())
        .collect();

    debug!("{workloads:?}");
    
    let req = pb::ResetWorkloadsRequest{
        cluster_id: kube.cluster_id().to_string(),
        workloads,
    };

    if let Err(err) = edgebit.reset_workloads(req).await {
        error!("Failed to send snapshot: {err}");
    }

    Ok(())
}

#[derive(Debug)]
struct Workload {
    container: Container,
    labels: HashMap<String, String>,
    machine_id: String,
}

impl Workload {
    fn new(container: Container, machine_id: String) -> Self {
        Self{
            container,
            labels: HashMap::new(),
            machine_id,
        }
    }

    fn add_labels(&mut self, kind: &str, labels: &BTreeMap<String, String>) {
        for (k, v) in labels {
            self.labels.insert(namespaced_label_key(kind, k), v.clone());
        }
    }

    fn set_name(&mut self, kind: &str, name: String) {
        let key = match kube_state::kind_alias(kind) {
            Some(alias) => format!("kube:{}:name", alias),
            None => format!("kube:{}:name", kind.to_lowercase()),
        };
        
        self.labels.insert(key, name);
    }

    fn into_pb(self) -> pb::WorkloadInstance {
        pb::WorkloadInstance {
            workload_id: self.container.container_id,
            workload: Some(pb::Workload{
                labels: self.labels,
                kind: Some(pb::workload::Kind::Container(pb::Container{
                        name: self.container.name,
                })),
            }),
            start_time: self.container.started_at.map(dt_to_timestamp),
            end_time: match self.container.finished_at {
                Some(dt) => Some(dt_to_timestamp(dt)),
                None => Some(TIMESTAMP_INFINITY),
            },
            image_id: self.container.image_id,
            image: Some(pb::Image{
                kind: Some(pb::image::Kind::Docker(pb::DockerImage{
                    tag: self.container.image,
                })),
            }),
            machine_id: self.machine_id,
        }
    }
}
 
fn make_workloads(kube: &KubeState) -> Vec<Workload> {
    let mut wls = Vec::new();

    for c in kube.containers() {
        if let Some(pod) = kube.get(kube_state::KIND_POD, &c.pod_uid) {
            let ns = pod.meta.namespace.clone()
                .unwrap_or("default".to_string());

            let machine_id = kube.machine_id(&c.node_name);

            match machine_id {
                Some(machine_id) => {
                    let mut wl = Workload::new(c.clone(), machine_id.to_string());
                    expand_labels(kube, &mut wl, kube_state::KIND_POD, pod);
                    wl.labels.insert("kube:namespace:name".to_string(), ns);

                    debug!("{wl:?}");
                    wls.push(wl);
                },

                None => {
                    warn!("machineID was not found for pod {}/{}", ns, pod.meta.name.clone().unwrap_or_default());
                }
            }
        }
    }

    wls
}

fn expand_labels(kube: &KubeState, wl: &mut Workload, kind: &str, resource: &KubeResource) {
    if let Some(ref name) = resource.meta.name {
        wl.set_name(kind, name.clone());
    }

    if let Some(ref labels) = resource.meta.labels {
        wl.add_labels(kind, &labels);
    }

    for owner_ref in resource.owner_refs() {
        if let Some(owner) = kube.get(&owner_ref.kind, &owner_ref.uid) {
            expand_labels(kube, wl, &owner_ref.kind, owner);
        }
    }
}

fn namespaced_label_key(kind: &str, key: &str) -> String {
    match kube_state::kind_alias(kind) {
        Some(alias) => format!("kube:{alias}:labels:{key}"),
        None => format!("kube:{}:labels:{key}", kind.to_lowercase()),
    }
}

fn stringify_labels(labels: &HashMap<String, String>) -> String {
    let labels:Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    labels.join("\n    ")
}

fn dt_to_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp{
        seconds: dt.timestamp(),
        nanos: 0,
    }
}
