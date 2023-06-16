use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use log::*;
use tokio::task::JoinHandle;
use tonic::codegen::InterceptedService;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};

pub mod pb {
    tonic::include_proto!("edgebit.agent.v1alpha");
}

use pb::inventory_service_client::InventoryServiceClient;
use pb::token_service_client::TokenServiceClient;

use crate::version::VERSION;

use self::pb::UpsertWorkloadsRequest;

const EXPIRATION_SLACK: Duration = Duration::from_secs(60);
const DEFAULT_EXPIRATION: Duration = Duration::from_secs(60 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct Client {
    inventory_svc: InventoryServiceClient<InterceptedService<Channel, AuthToken>>,
    sess_keeper_task: JoinHandle<()>,
}

impl Client {
    pub async fn connect(endpoint: Uri, deploy_token: String) -> Result<Self> {
        let channel = Channel::builder(endpoint).connect().await?;

        let (token, mut expiration) = enroll_loop(channel.clone(), deploy_token.clone()).await;

        let auth_token = AuthToken::new(&token);

        let inventory_svc =
            InventoryServiceClient::with_interceptor(channel.clone(), auth_token.clone());

        let sess_keeper_task = tokio::task::spawn(async move {
            while let Err(err) = refresh_loop(channel.clone(), auth_token.clone(), expiration).await
            {
                error!("Session renewal failed: {err}");

                // try re-enrolling
                let (token, exp) = enroll_loop(channel.clone(), deploy_token.clone()).await;
                auth_token.set(&token);
                expiration = exp;
            }
        });

        Ok(Self {
            inventory_svc,
            sess_keeper_task,
        })
    }

    pub async fn upsert_cluster(&mut self, cluster: pb::Cluster) -> Result<()> {
        self.inventory_svc
            .upsert_clusters(pb::UpsertClustersRequest {
                clusters: vec![cluster],
            })
            .await?;

        Ok(())
    }

    pub async fn upsert_machines(&mut self, req: pb::UpsertMachinesRequest) -> Result<()> {
        self.inventory_svc.upsert_machines(req).await?;
        Ok(())
    }

    pub async fn reset_workloads(&mut self, req: pb::ResetWorkloadsRequest) -> Result<()> {
        self.inventory_svc.reset_workloads(req).await?;
        Ok(())
    }

    pub async fn upsert_workloads(&mut self, req: UpsertWorkloadsRequest) -> Result<()> {
        self.inventory_svc.upsert_workloads(req).await?;
        Ok(())
    }

    pub async fn stop(self) {
        self.sess_keeper_task.abort();
        _ = self.sess_keeper_task.await;
    }
}

#[derive(Clone)]
struct AuthToken {
    inner: Arc<Mutex<AsciiMetadataValue>>,
}

impl AuthToken {
    fn new(token: &str) -> Self {
        let bearer = format_bearer(token);

        Self {
            inner: Arc::new(Mutex::new(bearer)),
        }
    }

    fn bearer(&self) -> AsciiMetadataValue {
        self.inner.lock().unwrap().clone()
    }

    fn set(&self, token: &str) {
        *self.inner.lock().unwrap() = format_bearer(token);
    }
}

impl Interceptor for AuthToken {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.bearer());
        Ok(request)
    }
}

fn format_bearer(val: &str) -> AsciiMetadataValue {
    // val must be ASCII
    format!("Bearer {val}").parse().unwrap()
}

async fn enroll(channel: Channel, deploy_token: String) -> Result<(String, SystemTime)> {
    let mut token_svc = TokenServiceClient::new(channel);

    let req = pb::EnrollClusterAgentRequest {
        deployment_token: Some(pb::Token {
            token_type: Some(pb::token::TokenType::ApiToken(deploy_token)),
        }),
        agent_version: VERSION.to_string(),
    };

    let resp = token_svc.enroll_cluster_agent(req).await?.into_inner();

    // ensure the token is ascii
    _ = AsciiMetadataValue::try_from(&resp.session_token)
        .map_err(|_| anyhow!("session token is not ASCII"))?;

    Ok((
        resp.session_token,
        get_expiration(resp.session_token_expiration),
    ))
}

async fn enroll_loop(channel: Channel, deploy_token: String) -> (String, SystemTime) {
    loop {
        match enroll(channel.clone(), deploy_token.clone()).await {
            Ok(tok) => return tok,
            Err(err) => {
                error!("Agent enrollment failed: {err}");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

async fn refresh_loop(
    channel: Channel,
    auth_token: AuthToken,
    mut expiration: SystemTime,
) -> Result<()> {
    let mut token_svc = TokenServiceClient::with_interceptor(channel, auth_token.clone());

    loop {
        let mut interval = expiration
            .duration_since(SystemTime::now())
            .unwrap_or_else(|_| {
                error!("Session expiration is in the past");
                DEFAULT_EXPIRATION
            });

        interval = interval.checked_sub(EXPIRATION_SLACK).unwrap_or(interval);

        info!("Next session renewal in {interval:?}");

        tokio::time::sleep(interval).await;

        let req = pb::GetSessionTokenRequest {
            refresh_token: String::new(),
            agent_version: VERSION.to_string(),
        };

        let resp = token_svc.get_session_token(req).await?.into_inner();

        // ensure the token is ascii
        _ = AsciiMetadataValue::try_from(&resp.session_token)
            .map_err(|_| anyhow!("session token is not ASCII"))?;

        auth_token.set(&resp.session_token);
        expiration = get_expiration(resp.session_token_expiration);

        info!("Session renewed");
    }
}

fn get_expiration(expiration: Option<prost_types::Timestamp>) -> SystemTime {
    match expiration {
        Some(expiration) => match SystemTime::try_from(expiration) {
            Ok(expiration) => expiration,
            Err(_) => {
                error!("Invalid session expiration time");
                SystemTime::now() + DEFAULT_EXPIRATION
            }
        },
        None => {
            error!("Session token is missing expiration");
            SystemTime::now() + DEFAULT_EXPIRATION
        }
    }
}
