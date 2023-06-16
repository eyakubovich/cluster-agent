use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{ContainerStatus, Namespace, Node, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::Metadata;
use kube_client::api::{ListParams, ObjectMeta};
use kube_client::Api;
use kube_runtime::watcher::Event;
use log::*;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::{Receiver, Sender};

pub const KIND_POD: &str = "Pod";
pub const KIND_DEPLOYMENT: &str = "Deployment";
pub const KIND_REPLICA_SET: &str = "ReplicaSet";
pub const KIND_STATEFUL_SET: &str = "StatefulSet";
pub const KIND_JOB: &str = "Job";
pub const KIND_CRON_JOB: &str = "CronJob";
pub const KIND_DAEMON_SET: &str = "DaemonSet";

const NS_KUBE_SYSTEM: &str = "kube-system";

#[derive(Clone, Debug)]
pub struct Machine {
    pub name: String,
    pub id: String,
}

impl From<Node> for Machine {
    fn from(node: Node) -> Self {
        let machine_id = node
            .status
            .and_then(|s| s.node_info)
            .map(|ni| ni.machine_id);

        Self {
            name: node.metadata.name.unwrap_or_default(),
            id: machine_id.unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Container {
    pub pod_uid: String,
    pub container_id: String,
    pub name: String,
    pub image: String,
    pub image_id: String,
    pub node_name: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl Container {
    fn reset(&mut self, new: &Container) -> bool {
        assert!(self.container_id == new.container_id);

        let changed = (&self.started_at, &self.finished_at) != (&new.started_at, &new.finished_at);
        if changed {
            self.started_at = new.started_at;
            self.finished_at = new.finished_at;
        }

        changed
    }

    fn terminate(&mut self) -> bool {
        if self.finished_at.is_none() {
            self.finished_at = Some(SystemTime::now().into());
            true
        } else {
            false
        }
    }
}

impl TryFrom<ContainerStatus> for Container {
    type Error = anyhow::Error;

    fn try_from(status: ContainerStatus) -> Result<Self> {
        if status.container_id.is_none() {
            return Err(anyhow!("container_id is missing"));
        }

        let (started_at, finished_at) = if let Some(state) = status.state {
            if let Some(running) = state.running {
                (running.started_at.map(|dt| dt.0), None)
            } else if let Some(terminated) = state.terminated {
                (
                    terminated.started_at.map(|dt| dt.0),
                    terminated.finished_at.map(|dt| dt.0),
                )
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(Self {
            pod_uid: String::new(),
            container_id: status.container_id.unwrap(),
            name: status.name,
            image: status.image,
            image_id: status.image_id,
            node_name: String::new(),
            started_at,
            finished_at,
        })
    }
}

#[derive(Clone, Debug)]
struct PodEntry {
    meta: KubeResource,
    containers: HashMap<String, Container>,
}

impl PodEntry {
    fn reset(&mut self, new: Pod) -> Vec<Container> {
        // assume meta is immutable
        let mut changed = Vec::new();
        let mut reality = HashMap::new();

        for (id, new_c) in PodEntry::from(new).containers {
            match self.containers.remove(&id) {
                Some(mut old_c) => {
                    // intersection: container might have changed
                    if old_c.reset(&new_c) {
                        changed.push(old_c.clone());
                    }
                    reality.insert(id, old_c);
                }
                None => {
                    // a container got added
                    changed.push(new_c.clone());
                    reality.insert(id, new_c);
                }
            }
        }

        // what is left in self.containers are the deleted ones
        for (_, mut old_c) in self.containers.drain() {
            if old_c.terminate() {
                changed.push(old_c);
            }
        }

        self.containers = reality;

        changed
    }

    fn terminate(self) -> Vec<Container> {
        self.containers
            .into_values()
            .filter_map(|mut c| if c.terminate() { Some(c) } else { None })
            .collect()
    }
}

impl From<Pod> for PodEntry {
    fn from(pod: Pod) -> Self {
        assert!(pod.metadata.uid.is_some());
        let meta = KubeResource::from(pod.metadata);

        let mut containers = HashMap::new();

        if let Some(status) = pod.status {
            if let Some(ics) = status.init_container_statuses {
                for s in ics {
                    if let Ok(mut c) = Container::try_from(s) {
                        c.pod_uid = meta.uid.clone();
                        c.node_name = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.node_name.clone())
                            .unwrap_or_default();

                        containers.insert(c.container_id.clone(), c);
                    }
                }
            }

            if let Some(cs) = status.container_statuses {
                for s in cs {
                    if let Ok(mut c) = Container::try_from(s) {
                        c.pod_uid = meta.uid.clone();
                        c.node_name = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.node_name.clone())
                            .unwrap_or_default();

                        containers.insert(c.container_id.clone(), c);
                    }
                }
            }
        }

        Self { meta, containers }
    }
}

#[derive(Clone, Debug)]
pub struct KubeResource {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub owner_refs: Vec<OwnerReference>,
}

impl From<ObjectMeta> for KubeResource {
    fn from(meta: ObjectMeta) -> Self {
        assert!(meta.uid.is_some());

        Self {
            uid: meta.uid.unwrap(),
            namespace: meta.namespace.unwrap_or("default".to_string()),
            name: meta.name.unwrap_or_default(),
            labels: meta.labels.unwrap_or_default(),
            owner_refs: meta.owner_references.unwrap_or_default(),
        }
    }
}

#[derive(Default)]
struct Resources {
    sys_ns_id: String,
    nodes: HashMap<String, String>, // nodeName -> machineID
    pods: HashMap<String, PodEntry>,
    resources: HashMap<&'static str, Vec<KubeResource>>,
}

impl Resources {
    fn upsert(&mut self, kind: &str, resource: ObjectMeta) {
        // unwrap() below is ok b/c kind is a compile time value.
        let resources = self.resources.get_mut(kind).unwrap();

        let found = resources
            .iter_mut()
            .find(|r| r.uid == *resource.uid.as_ref().unwrap());

        match found {
            Some(item) => *item = KubeResource::from(resource),
            None => resources.push(KubeResource::from(resource)),
        };
    }

    fn delete(&mut self, kind: &str, uid: &str) {
        // unwrap() below is ok b/c kind is a compile time value.
        let resources = self.resources.get_mut(kind).unwrap();

        let pos = resources.iter().position(|r| r.uid == uid);

        if let Some(pos) = pos {
            resources.remove(pos);
        }
    }

    fn upsert_node(&mut self, node: Node) -> Machine {
        let machine = Machine::from(node);

        self.nodes.insert(machine.name.clone(), machine.id.clone());

        machine
    }

    fn delete_node(&mut self, name: &str) {
        self.nodes.remove(name);
    }

    fn clear_nodes(&mut self) {
        self.nodes.clear();
    }

    // returns the list of changed containers in the pod
    fn upsert_pod(&mut self, pod: Pod) -> Vec<Container> {
        if pod.metadata.uid.is_some() {
            match self.pods.get_mut(pod.metadata.uid.as_ref().unwrap()) {
                Some(existing) => existing.reset(pod),
                None => {
                    let pod = PodEntry::from(pod);

                    let containers = pod.containers.values().cloned().collect();

                    self.pods.insert(pod.meta.uid.clone(), pod);
                    containers
                }
            }
        } else {
            Vec::new()
        }
    }

    // returns the list of containers in the deleted pod
    fn delete_pod(&mut self, uid: &str) -> Vec<Container> {
        match self.pods.remove(uid) {
            Some(pod) => pod.terminate(),
            None => Vec::new(),
        }
    }

    fn clear_pods(&mut self) {
        self.pods.clear();
    }

    // returns changed containers
    fn reset_pods(&mut self, new_pods: Vec<Pod>) -> Vec<Container> {
        let mut changed = Vec::new();
        let mut reality = HashMap::new();

        for new_pod in new_pods {
            if new_pod.metadata.uid.is_some() {
                match self.pods.remove(new_pod.metadata.uid.as_ref().unwrap()) {
                    Some(mut old_pod) => {
                        // intersection: pod might have changed
                        changed.append(&mut old_pod.reset(new_pod));
                        reality.insert(old_pod.meta.uid.clone(), old_pod);
                    }
                    None => {
                        // a pod got added
                        let new_pod = PodEntry::from(new_pod);
                        changed.extend(new_pod.containers.values().cloned());
                        reality.insert(new_pod.meta.uid.clone(), new_pod);
                    }
                }
            }
        }

        // what is left in self.pods are the deleted pods
        for (_, old_pod) in self.pods.drain() {
            changed.append(&mut old_pod.terminate());
        }

        self.pods = reality;

        changed
    }
}

pub enum KubeEvent {
    ContainersUpdated(Vec<Container>),

    MachinesAdded(Vec<Machine>),
}

pub struct KubeState {
    client: kube_client::Client,
    resources: Arc<Mutex<Resources>>,
    events: Sender<KubeEvent>,
}

impl KubeState {
    pub async fn with_defaults() -> Result<(Self, Receiver<KubeEvent>)> {
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        Ok((
            Self {
                client: kube_client::Client::try_default().await?,
                resources: Arc::new(Mutex::new(Resources::default())),
                events: tx,
            },
            rx,
        ))
    }

    pub async fn load_all(&self) -> Result<()> {
        self.load_cluster_id().await?;
        self.load_nodes().await?;
        self.load_deployments().await?;
        self.load_replica_sets().await?;
        self.load_stateful_sets().await?;
        self.load_jobs().await?;
        self.load_cron_jobs().await?;
        self.load_daemon_sets().await?;
        self.load_pods().await?;

        Ok(())
    }

    pub async fn load_nodes(&self) -> Result<()> {
        let api: Api<Node> = Api::all(self.client.clone());

        let nodes = api.list(&ListParams::default()).await?;

        let mut resources = self.resources.lock().unwrap();

        resources.clear_nodes();

        for node in nodes {
            resources.upsert_node(node);
        }

        Ok(())
    }

    pub async fn load_pods(&self) -> Result<()> {
        let api: Api<Pod> = Api::all(self.client.clone());

        let pods = api.list(&ListParams::default()).await?;

        let mut resources = self.resources.lock().unwrap();

        resources.clear_pods();

        for pod in pods {
            resources.upsert_pod(pod);
        }

        Ok(())
    }

    pub async fn load_deployments(&self) -> Result<()> {
        self.set_resources(KIND_DEPLOYMENT, self.fetch_all::<Deployment>().await?);
        Ok(())
    }

    pub async fn load_replica_sets(&self) -> Result<()> {
        self.set_resources(KIND_REPLICA_SET, self.fetch_all::<ReplicaSet>().await?);
        Ok(())
    }

    pub async fn load_stateful_sets(&self) -> Result<()> {
        self.set_resources(KIND_STATEFUL_SET, self.fetch_all::<StatefulSet>().await?);
        Ok(())
    }

    pub async fn load_jobs(&self) -> Result<()> {
        self.set_resources(KIND_JOB, self.fetch_all::<Job>().await?);
        Ok(())
    }

    pub async fn load_cron_jobs(&self) -> Result<()> {
        self.set_resources(KIND_CRON_JOB, self.fetch_all::<CronJob>().await?);
        Ok(())
    }

    pub async fn load_daemon_sets(&self) -> Result<()> {
        self.set_resources(KIND_DAEMON_SET, self.fetch_all::<DaemonSet>().await?);
        Ok(())
    }

    pub async fn load_cluster_id(&self) -> Result<()> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        let sys_ns_id = api
            .get(NS_KUBE_SYSTEM)
            .await?
            .metadata
            .uid
            .ok_or(anyhow!("UID missing for {NS_KUBE_SYSTEM} namespace"))?;

        debug!("Loaded ClusterID: {sys_ns_id}");

        self.resources.lock().unwrap().sys_ns_id = sys_ns_id;

        Ok(())
    }

    async fn fetch_all<K>(&self) -> Result<Vec<KubeResource>>
    where
        K: Clone + DeserializeOwned + Debug + Metadata<Ty = ObjectMeta>,
        <K as kube_client::Resource>::DynamicType: Default,
    {
        let api: Api<K> = Api::all(self.client.clone());
        let all = api.list_metadata(&ListParams::default()).await?;

        Ok(all
            .into_iter()
            .map(|o| KubeResource::from(o.metadata))
            .collect())
    }

    pub fn get(&self, kind: &str, uid: &str) -> Option<KubeResource> {
        let resources = self.resources.lock().unwrap();

        if kind == KIND_POD {
            resources.pods.get(uid).map(|p| p.meta.clone())
        } else {
            match resources.resources.get(kind) {
                Some(resources) => resources.iter().find(|r| r.uid == uid).cloned(),
                None => None,
            }
        }
    }

    pub fn containers(&self) -> Vec<Container> {
        let resources = self.resources.lock().unwrap();

        let mut containers = Vec::new();

        for pod in resources.pods.values() {
            containers.extend(pod.containers.values().cloned());
        }

        containers
    }

    pub fn machine_id(&self, node_name: &str) -> Option<String> {
        self.resources.lock().unwrap().nodes.get(node_name).cloned()
    }

    pub fn cluster_id(&self) -> String {
        self.resources.lock().unwrap().sys_ns_id.clone()
    }

    pub fn machines(&self) -> Vec<Machine> {
        self.resources
            .lock()
            .unwrap()
            .nodes
            .iter()
            .map(|(name, id)| Machine {
                id: id.to_string(),
                name: name.to_string(),
            })
            .collect()
    }

    pub fn dump(&self) {
        let resources = self.resources.lock().unwrap();

        for (kind, resources) in &resources.resources {
            println!("{kind}:");
            for res in resources {
                let labels: Vec<String> =
                    res.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
                let labels = labels.join(", ");

                println!("    {}: [ {labels} ]", res.name);
            }
        }
    }

    fn set_resources(&self, kind: &'static str, resources: Vec<KubeResource>) {
        self.resources
            .lock()
            .unwrap()
            .resources
            .insert(kind, resources);
    }

    pub async fn watch<K>(&self, kind: &'static str) -> Result<()>
    where
        K: kube_client::Resource + Clone + DeserializeOwned + Debug + Send + 'static,
        K::DynamicType: Default,
    {
        let api: Api<K> = Api::all(self.client.clone());

        kube_runtime::watcher::metadata_watcher(api, kube_runtime::watcher::Config::default())
            .try_for_each(|e| async move {
                match e {
                    Event::Applied(r) => {
                        if r.metadata.uid.is_some() {
                            self.resources.lock().unwrap().upsert(kind, r.metadata);
                        }
                    }
                    Event::Deleted(r) => {
                        if let Some(uid) = r.metadata.uid {
                            self.resources.lock().unwrap().delete(kind, &uid);
                        }
                    }
                    Event::Restarted(rs) => {
                        let resources = rs
                            .into_iter()
                            .map(|r| KubeResource::from(r.metadata))
                            .collect();

                        self.set_resources(kind, resources)
                    }
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    pub async fn watch_nodes(&self) -> Result<()> {
        let api: Api<Node> = Api::all(self.client.clone());

        let events = self.events.clone();

        kube_runtime::watcher::watcher(api, kube_runtime::watcher::Config::default())
            .try_for_each(move |e| {
                let events = events.clone();

                async move {
                    match e {
                        Event::Applied(node) => {
                            let machine = self.resources.lock().unwrap().upsert_node(node);

                            let evt = KubeEvent::MachinesAdded(vec![machine]);
                            _ = events.send(evt).await;
                        }
                        Event::Deleted(node) => {
                            if let Some(name) = node.metadata.name {
                                self.resources.lock().unwrap().delete_node(&name);
                            }
                        }
                        Event::Restarted(nodes) => {
                            let mut resources = self.resources.lock().unwrap();

                            resources.clear_nodes();

                            for node in nodes {
                                resources.upsert_node(node);
                            }
                        }
                    }

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }

    pub async fn watch_pods(&self) -> Result<()> {
        let api: Api<Pod> = Api::all(self.client.clone());

        let events = self.events.clone();

        kube_runtime::watcher::watcher(api, kube_runtime::watcher::Config::default())
            .try_for_each(move |e| {
                let events = events.clone();

                async move {
                    match e {
                        Event::Applied(pod) => {
                            let containers = self.resources.lock().unwrap().upsert_pod(pod);

                            if !containers.is_empty() {
                                _ = events.send(KubeEvent::ContainersUpdated(containers)).await;
                            }
                        }
                        Event::Deleted(pod) => {
                            if let Some(uid) = pod.metadata.uid {
                                let containers = self.resources.lock().unwrap().delete_pod(&uid);

                                if !containers.is_empty() {
                                    _ = events.send(KubeEvent::ContainersUpdated(containers)).await;
                                }
                            }
                        }
                        Event::Restarted(pods) => {
                            let containers = self.resources.lock().unwrap().reset_pods(pods);

                            if !containers.is_empty() {
                                _ = events.send(KubeEvent::ContainersUpdated(containers)).await;
                            }
                        }
                    }

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }

    pub async fn watch_all(&self) -> Result<()> {
        _ = tokio::join!(
            self.watch_nodes(),
            self.watch_pods(),
            self.watch::<Deployment>(KIND_DEPLOYMENT),
            self.watch::<ReplicaSet>(KIND_REPLICA_SET),
            self.watch::<StatefulSet>(KIND_STATEFUL_SET),
            self.watch::<Job>(KIND_JOB),
            self.watch::<CronJob>(KIND_CRON_JOB),
            self.watch::<DaemonSet>(KIND_DAEMON_SET),
        );

        Ok(())
    }
}

pub fn kind_alias(kind: &str) -> Option<&'static str> {
    Some(match kind {
        KIND_POD => "pod",
        KIND_DEPLOYMENT => "deploy",
        KIND_REPLICA_SET => "rs",
        KIND_STATEFUL_SET => "sts",
        KIND_JOB => "job",
        KIND_CRON_JOB => "cj",
        KIND_DAEMON_SET => "ds",
        _ => return None,
    })
}
