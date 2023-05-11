use std::path::PathBuf;
use std::sync::{Mutex, Arc};
use std::time::{SystemTime, Duration};

use anyhow::{Result};
use log::*;
use tonic::{Request, Status};
use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Uri};
use tonic::metadata::AsciiMetadataValue;
use tokio::task::JoinHandle;

pub mod pb {
    tonic::include_proto!("edgebit.agent.v1alpha");
}

use pb::token_service_client::TokenServiceClient;
use pb::inventory_service_client::InventoryServiceClient;

use crate::version::VERSION;

const TOKEN_FILE: &str = "/var/lib/edgebit/token";
const EXPIRATION_SLACK: Duration = Duration::from_secs(60);
const DEFAULT_EXPIRATION: Duration = Duration::from_secs(60*60);

struct AuthInterceptor {
    token: AuthToken,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request.metadata_mut().insert("authorization", self.token.get());
        Ok(request)
    }
}

pub struct Client {
    inventory_svc: InventoryServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    sess_keeper_task: JoinHandle<()>,
}

impl Client {
    pub async fn connect(endpoint: Uri, deploy_token: String, hostname: String) -> Result<Self> {
        let channel = Channel::builder(endpoint)
            .connect()
            .await?;

        let sess_keeper = SessionKeeper::new(channel.clone(), deploy_token, hostname).await?;
        let auth_interceptor = AuthInterceptor{token: sess_keeper.get_auth_token()};
        let sess_keeper_task = tokio::task::spawn(sess_keeper.refresh_loop());
        let inventory_svc = InventoryServiceClient::with_interceptor(channel, auth_interceptor);

        Ok(Self{
            inventory_svc,
            sess_keeper_task,
        })
    }

    pub async fn upsert_workload(&mut self, workload: pb::UpsertWorkloadRequest) -> Result<()> {
        self.inventory_svc.upsert_workload(workload).await?;
        Ok(())
    }

    pub async fn reset_workloads(&mut self) -> Result<()> {
        self.inventory_svc.reset_workloads(pb::ResetWorkloadsRequest{}).await?;
        Ok(())
    }

    pub async fn stop(self) {
        self.sess_keeper_task.abort();
        _ = self.sess_keeper_task.await;
    }
}

#[derive(Clone)]
struct AuthToken {
    token: Arc<Mutex<AsciiMetadataValue>>,
}

impl AuthToken {
    fn new(val: &str) -> Result<Self> {
        let token = format_bearer(val)?;

        Ok(Self {
            token: Arc::new(Mutex::new(token)),
        })
    }

    fn get(&self) -> AsciiMetadataValue {
        self.token.lock()
            .unwrap()
            .clone()
    }

    fn set(&self, val: &str) -> Result<()> {
        let mut lk = self.token.lock().unwrap();
        *lk = format_bearer(val)?;
        Ok(())
    }
}

fn format_bearer(val: &str) -> Result<AsciiMetadataValue> {
    Ok(format!("Bearer {val}").parse()?)
}

struct RefreshToken {
    token: String,
}

impl RefreshToken {
    fn new(token: String) -> Self {
        Self{
            token,
        }
    }
    fn load() -> Result<Self> {
        let token = std::fs::read_to_string(TOKEN_FILE)?;
        Ok(Self{token})
    }

    fn save(&self) -> Result<()> {
        let token_file = PathBuf::from(TOKEN_FILE);
        let dir = token_file.parent().unwrap();
        std::fs::create_dir_all(dir)?;
        Ok(std::fs::write(TOKEN_FILE, &self.token)?)
    }

    fn get(&self) -> String {
        self.token.clone()
    }
}

struct SessionKeeper {
    refresh_token: RefreshToken,
    auth_token: AuthToken,
    expiration: SystemTime,
    channel: Channel,
}

impl SessionKeeper {
    async fn new(channel: Channel, deploy_token: String, hostname: String) -> Result<Self> {
        let mut token_svc = TokenServiceClient::new(channel.clone());

        let (refresh_token, session_token, expiration) = match RefreshToken::load() {
            Ok(refresh_token) => {
                let req = pb::GetSessionTokenRequest{
                    refresh_token: refresh_token.get(),
                    agent_version: VERSION.to_string(),
                };

                let resp = token_svc.get_session_token(req).await?
                    .into_inner();

                (refresh_token, resp.session_token, resp.session_token_expiration)
            },
            Err(_) => {
                let req = pb::EnrollAgentRequest{
                    deployment_token: deploy_token,
                    hostname,
                    agent_version: VERSION.to_string(),
                };

                let resp = token_svc.enroll_agent(req)
                    .await?
                    .into_inner();

                let refresh_token = RefreshToken::new(resp.refresh_token);
                refresh_token.save()
                    .unwrap_or_else(|err| {
                        error!("Error saving agent token: {err}");
                    });

                (refresh_token, resp.session_token, resp.session_token_expiration)
            }
        };

        let auth_token = AuthToken::new(&session_token)?;
        let expiration = get_expiration(expiration);

        Ok(Self{
            refresh_token,
            auth_token,
            expiration,
            channel,
        })
    }

    fn get_auth_token(&self) -> AuthToken {
        self.auth_token.clone()
    }

    async fn refresh_loop(self) {
        let mut token_svc = TokenServiceClient::new(self.channel);
        let mut expiration = self.expiration;

        loop {
            let mut interval = expiration.duration_since(SystemTime::now())
                .unwrap_or_else(|_| {
                    error!("Session expiration is in the past");
                    DEFAULT_EXPIRATION
                });

            interval = interval.checked_sub(EXPIRATION_SLACK)
                .unwrap_or(interval);

            info!("Sleeping for {interval:?}");

            tokio::time::sleep(interval).await;

            let req = pb::GetSessionTokenRequest{
                refresh_token: self.refresh_token.get(),
                agent_version: VERSION.to_string(),
            };

            expiration = match token_svc.get_session_token(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    self.auth_token.set(&resp.session_token).unwrap();
                    info!("Session renewed");
                    get_expiration(resp.session_token_expiration)
                },
                Err(err) => {
                    error!("Session renewal failed: {err}");
                    SystemTime::now() + EXPIRATION_SLACK
                }
            }
        }
    }
}

fn get_expiration(expiration: Option<prost_types::Timestamp>) -> SystemTime {
    match expiration {
        Some(expiration) => {
            match SystemTime::try_from(expiration) {
                Ok(expiration) => expiration,
                Err(_) => {
                    error!("Invalid session expiration time");
                    SystemTime::now() + DEFAULT_EXPIRATION
                }

            }
        },
        None => {
            error!("Session token is missing expiration");
            SystemTime::now() + DEFAULT_EXPIRATION
        }
    }
}