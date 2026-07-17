use google_agones_crds::v1::fleet_autoscaler::FleetAutoscalerPolicySpec;
use kube::core::ObjectMeta;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSpec<T> {
    /// Common metadata to add to the created objects
    pub metadata: Option<ObjectMeta>,

    /// The spec of the object to create from the template
    pub spec: T,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ImageOverrideSpec {
    /// Complete name of the image, including the repository name
    /// and tag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Policy about when to pull the image
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_policy: Option<String>,

    ///  A list of secrets to use to pull the image
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_secrets: Option<Vec<k8s_openapi::api::core::v1::LocalObjectReference>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FleetAutoscalingSpec {
    pub agones_policy: Option<FleetAutoscalerPolicySpec>,
}

/// Overrides for a Kubernetes container probe (readiness or liveness).
/// This is distinct from Agones' `GameServerHealthSpec`, which only
/// controls the Agones sidecar's own health checks: this spec controls
/// the actual `readinessProbe`/`livenessProbe` set on the game
/// container itself (e.g. the `probe-readiness.sh` exec probe).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProbeOverrideSpec {
    /// Number of seconds after the container has started before the
    /// probe is initiated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_delay_seconds: Option<i32>,

    /// How often (in seconds) to perform the probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_seconds: Option<i32>,

    /// Number of seconds after which the probe times out
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// Minimum consecutive failures for the probe to be considered
    /// failed after having succeeded
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<i32>,

    /// Minimum consecutive successes for the probe to be considered
    /// successful after having failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_threshold: Option<i32>,
}

impl ProbeOverrideSpec {
    /// Applies this override on top of `base`, keeping `base`'s value
    /// for any field left unset in `self`.
    pub fn merged_over(&self, base: &ProbeOverrideSpec) -> ProbeOverrideSpec {
        ProbeOverrideSpec {
            initial_delay_seconds: self.initial_delay_seconds.or(base.initial_delay_seconds),
            period_seconds: self.period_seconds.or(base.period_seconds),
            timeout_seconds: self.timeout_seconds.or(base.timeout_seconds),
            failure_threshold: self.failure_threshold.or(base.failure_threshold),
            success_threshold: self.success_threshold.or(base.success_threshold),
        }
    }
}
