use shulker_crds::v1alpha1::minecraft_cluster::MinecraftCluster;

/// Resolves the effective online-mode for a `ProxyFleet` or `MinecraftServer`,
/// given its own optional override and the `MinecraftCluster` it is enrolled
/// in. The resource-level override always wins; falling back to the cluster's
/// default, itself defaulting to `true` (Mojang authentication enabled)
pub fn resolve_online_mode(cluster: &MinecraftCluster, override_: Option<bool>) -> bool {
    override_.or(cluster.spec.online_mode).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use shulker_crds::v1alpha1::minecraft_cluster::{MinecraftCluster, MinecraftClusterSpec};

    use crate::reconcilers::minecraft_cluster::fixtures::TEST_CLUSTER;

    #[test]
    fn defaults_to_true_when_unset_everywhere() {
        // G
        let mut cluster = TEST_CLUSTER.clone();
        cluster.spec.online_mode = None;

        // W
        let online_mode = super::resolve_online_mode(&cluster, None);

        // T
        assert!(online_mode);
    }

    #[test]
    fn inherits_cluster_default() {
        // G
        let mut cluster = TEST_CLUSTER.clone();
        cluster.spec.online_mode = Some(false);

        // W
        let online_mode = super::resolve_online_mode(&cluster, None);

        // T
        assert!(!online_mode);
    }

    #[test]
    fn resource_override_wins_over_cluster_default() {
        // G
        let mut cluster = TEST_CLUSTER.clone();
        cluster.spec.online_mode = Some(false);

        // W
        let online_mode = super::resolve_online_mode(&cluster, Some(true));

        // T
        assert!(online_mode);
    }

    #[test]
    fn resource_override_wins_when_cluster_unset() {
        // G
        let cluster = MinecraftCluster::new(
            "my-cluster",
            MinecraftClusterSpec {
                online_mode: None,
                network_admins: None,
                redis: None,
                external_servers: None,
            },
        );

        // W
        let online_mode = super::resolve_online_mode(&cluster, Some(false));

        // T
        assert!(!online_mode);
    }
}
