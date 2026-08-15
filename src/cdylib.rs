//! cdylib sync bridge — adapts the async [`AmqpBackendPlugin`] onto the sync
//! FFI trait the cdylib vtable expects ([`SyncBackendPlugin`]). A private
//! multi-thread runtime `block_on`s the async methods (lapin's tokio reactor
//! runs on it); the make-time [`HostHandle`] is wrapped as
//! `Arc<dyn BackendHost>` for `register_profile` and installed on the inner
//! plugin for observability. AMQP request/reply is single-response, so it
//! inherits the SDK's single-`Done` streaming default.

use std::sync::Arc;

use mcpg_plugin_protocol::{
    BackendError, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
};
use mcpg_plugin_sdk::ffi::SyncBackendPlugin;
use mcpg_plugin_sdk::{HostHandle, HostHandleBackendHost};

use crate::AmqpBackendPlugin;

fn build_bridge_runtime(thread_name: &str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(thread_name.to_owned())
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("amqp cdylib: tokio runtime init failed: {e}"))
}

/// `SyncBackendPlugin` bridge over [`AmqpBackendPlugin`].
pub struct AmqpBackendCdylib {
    inner: AmqpBackendPlugin,
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    rt: tokio::runtime::Runtime,
}

impl AmqpBackendCdylib {
    /// Infallible cdylib factory. `config_json` is ignored — AMQP carries no
    /// plugin-level config (per-binding uri / op arrive via `register_profile`).
    pub fn from_host_config(_config_json: &str, host: HostHandle) -> Self {
        let inner = AmqpBackendPlugin::new();
        let _installed = inner.set_host_handle(host.clone());
        Self {
            inner,
            host: Arc::new(HostHandleBackendHost::new(host)),
            rt: build_bridge_runtime("mcpg-backend-amqp"),
        }
    }
}

impl SyncBackendPlugin for AmqpBackendCdylib {
    fn manifest(&self) -> &PluginManifest {
        BackendPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        BackendPlugin::kind(&self.inner)
    }

    fn register_profile(
        &self,
        profile_name: &str,
        spec: &serde_json::Value,
    ) -> Result<(), BackendError> {
        self.rt.block_on(BackendPlugin::register_profile(
            &self.inner,
            profile_name,
            spec,
            Arc::clone(&self.host),
        ))
    }

    fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        self.rt
            .block_on(BackendPlugin::execute(&self.inner, profile_name, request))
    }

    fn audit_metadata(&self, profile_name: &str) -> serde_json::Map<String, serde_json::Value> {
        BackendPlugin::audit_metadata(&self.inner, profile_name)
    }
}

// cdylib export — one `backend` entity under `dev.mcpg.backend.amqp`.
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.backend.amqp",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    // AMQP: pipeline-capable (a `kind: amqp` pipeline step), no dynamic tool
    // list, health is advisory (Skip — broker liveness is tracked by the
    // reused lapin connection), label defaults to the kind. `uri` is a
    // transport-only connection fact (its password resolves at config load via
    // `${env.X}` / `vault://`), so the gateway's generic spec-walk asserts no
    // `cred://` lands there — matching the plugin's own `register_profile`
    // reject.
    backend_profile: ::mcpg_plugin_protocol::manifest::BackendProfile {
        pipeline_capable: true,
        transport_only_fields: ::std::vec!["/uri".to_owned()],
        ..::core::default::Default::default()
    },
    entities: [
        backend as binding {
            inner_name: "",
            plugin_type: AmqpBackendCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                AmqpBackendCdylib::from_host_config(cfg, host),
        },
    ],
}
