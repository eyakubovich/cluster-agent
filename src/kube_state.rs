use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use anyhow::{Result, anyhow};
use serde::de::DeserializeOwned;
use k8s_openapi::Metadata;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::api::core::v1::{Pod, ContainerStatus};
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet, StatefulSet, DaemonSet};
use k8s_openapi::api::batch::v1::{Job, CronJob};
use kube_client::api::{ObjectMeta, ListParams};
use kube_client::Api;
use chrono::{DateTime, Utc};

pub const KIND_POD: &str = "Pod";
pub const KIND_DEPLOYMENT: &str = "Deployment";
pub const KIND_REPLICA_SET: &str = "ReplicaSet";
pub const KIND_STATEFUL_SET: &str = "StatefulSet";
pub const KIND_JOB: &str = "Job";
pub const KIND_CRON_JOB: &str = "CronJob";
pub const KIND_DAEMON_SET: &str = "DaemonSet";

#[derive(Clone, Debug)]
pub struct Container {
    pub pod_uid: String,
    pub container_id: String,
    pub name: String,
    pub image: String,
    pub image_id: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
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
                (terminated.started_at.map(|dt| dt.0), terminated.finished_at.map(|dt| dt.0))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(Self{
            pod_uid: String::new(),
            container_id: status.container_id.unwrap(),
            name: status.name,
            image: status.image,
            image_id: status.image_id,
            started_at,
            finished_at,
        })
    }
}

#[derive(Clone, Debug)]
pub struct KubeResource {
    pub meta: ObjectMeta,
}

impl KubeResource {
    pub fn owner_refs(&self) -> &[OwnerReference] {
        match self.meta.owner_references {
            Some(ref refs) => refs,
            None => &[],
        }
    }
}

pub struct KubeState {
    client: kube_client::Client,
    containers: Vec<Container>,
    resources: HashMap<&'static str, Vec<KubeResource>>,
}

impl KubeState {
    pub async fn with_defaults() -> Result<Self> {
        Ok(Self{
            client: kube_client::Client::try_default().await?,
            containers: Vec::new(),
            resources: HashMap::new(),
        })
    }

    pub async fn load_all(&mut self) -> Result<()> {
        self.load_pods().await?;
        self.load_deployments().await?;
        self.load_replica_sets().await?;
        self.load_stateful_sets().await?;
        self.load_jobs().await?;
        self.load_cron_jobs().await?;
        self.load_daemon_sets().await?;

        Ok(())
    }

    pub async fn load_pods(&mut self) -> Result<()> {
        let api: Api<Pod> = Api::all(self.client.clone());
        let mut pods = Vec::new();

        for pod in api.list(&ListParams::default()).await? {
            if let Some(status) = pod.status {
                if let Some(ics) = status.init_container_statuses {
                    for s in ics {
                        if let Ok(mut c) = Container::try_from(s) {
                            c.pod_uid = pod.metadata.uid.clone()
                                .unwrap_or_default();

                            self.containers.push(c);
                        }
                    }
                }

                if let Some(cs) = status.container_statuses {
                    for s in cs {
                        if let Ok(mut c) = Container::try_from(s) {
                            c.pod_uid = pod.metadata.uid.clone()
                                .unwrap_or_default();

                            self.containers.push(c);
                        }
                    }
                }
            }

            pods.push(pod.metadata);
        }

        self.resources.insert(KIND_POD, self.fetch_all::<Pod>().await?);
        Ok(())
    }

    pub async fn load_deployments(&mut self) -> Result<()> {
        self.resources.insert(KIND_DEPLOYMENT, self.fetch_all::<Deployment>().await?);
        Ok(())
    }

    pub async fn load_replica_sets(&mut self) -> Result<()> {
        self.resources.insert(KIND_REPLICA_SET, self.fetch_all::<ReplicaSet>().await?);
        Ok(())
    }

    pub async fn load_stateful_sets(&mut self) -> Result<()> {
        self.resources.insert(KIND_STATEFUL_SET, self.fetch_all::<StatefulSet>().await?);
        Ok(())
    }

    pub async fn load_jobs(&mut self) -> Result<()> {
        self.resources.insert(KIND_JOB, self.fetch_all::<Job>().await?);
        Ok(())
    }

    pub async fn load_cron_jobs(&mut self) -> Result<()> {
        self.resources.insert(KIND_CRON_JOB, self.fetch_all::<CronJob>().await?);
        Ok(())
    }

    pub async fn load_daemon_sets(&mut self) -> Result<()> {
        self.resources.insert(KIND_DAEMON_SET, self.fetch_all::<DaemonSet>().await?);
        Ok(())
    }

    async fn fetch_all<K>(&self) -> Result<Vec<KubeResource>>
    where K: Clone + DeserializeOwned + Debug + Metadata<Ty = ObjectMeta>,
        <K as kube_client::Resource>::DynamicType: Default
    {
        let api: Api<K> = Api::all(self.client.clone());
        let all = api.list_metadata(&ListParams::default()).await?;

        Ok(all.into_iter()
            .map(|o| KubeResource{ meta: o.metadata })
            .collect())
    }

    pub fn get<'a>(&'a self, kind: &str, uid: &str) -> Option<&'a KubeResource> {
        let key = Some(uid.to_string());

        match self.resources.get(kind) {
            Some(resources) => {
                resources.iter()
                    .find(|r| r.meta.uid == key)
            },
            None => None,
        }
    }

    pub fn containers(&self) -> &[Container] {
        &self.containers
    }

    pub fn dump(&self) {
        for (kind, resources) in &self.resources {
            println!("{kind}:");
            for res in resources {
                let labels: Vec<String> = res.meta.labels.as_ref()
                    .unwrap_or(&BTreeMap::new())
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                let labels = labels.join(", ");

                println!("    {}: [ {labels} ]", res.meta.name.as_ref().unwrap_or(&"".to_string()));
            }
        }
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