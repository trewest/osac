from __future__ import annotations

import contextlib
import os
import subprocess
import tempfile

import pytest

from tests.e2e.caas.add_on_operators import (
    _assert_subscriptions_and_csvs,
    _ensure_published_operators,
    _restore_operator_publication,
    _wait_for_api_operators_installed,
    _wait_for_hosted_cluster_references,
)
from tests.e2e.caas.sanity.test_cluster_create import (
    RHCOS_IMAGE,
    TEST_RELEASE_IMAGE,
    _assert_cluster_deleted,
    assert_cluster_with_two_node_sets_available,
)
from tests.e2e.core.fulfillment_trust import assert_cluster_trust, assert_management_tls
from tests.e2e.core.grpc_client import GRPCClient
from tests.e2e.core.helpers import unique_name, wait_for_cluster_order_condition, wait_for_hosted_cluster_kubeconfig
from tests.e2e.core.k8s_client import K8sClient
from tests.e2e.core.metering import MeteringCollector
from tests.e2e.core.osac_cli import OsacCLI

pytestmark = pytest.mark.regression


@pytest.mark.metering
def test_cluster_create_with_two_node_sets(
    cli: OsacCLI,
    grpc: GRPCClient,
    private_grpc: GRPCClient,
    k8s_hub_client: K8sClient,
    cluster_template: str,
    pull_secret_name: str,
    ssh_public_key_path: str,
    metering: MeteringCollector,
) -> None:
    """Verify NodePool replicas stay isolated when a cluster has two BMaaS node sets."""

    operator_names = ("node-feature-discovery", "nvidia-gpu")
    operator_specs, publication_states, _ = _ensure_published_operators(cli, private_grpc, operator_names)
    instance_types = {"compute": "ci-worker-bm", "gpu": "ci-worker-bm-gpu"}
    uuid = ""
    try:
        for instance_type in instance_types.values():
            private_grpc.ensure_bare_metal_instance_type(name=instance_type)

        version = private_grpc.ensure_cluster_version(
            version="4.22.0-rhcos",
            image=TEST_RELEASE_IMAGE,
            disk_image=private_grpc.ensure_disk_image(name="rhcos-4-22", source_ref=RHCOS_IMAGE),
        )
        name = unique_name("e2e-cluster-two-node-sets")
        uuid = cli.create_cluster(
            name=name,
            template=cluster_template,
            version=version["name"],
            node_sets={
                node_set: {"size": 1, "baremetal_instance_type": {"name": instance_type}}
                for node_set, instance_type in instance_types.items()
            },
            pull_secret=pull_secret_name,
            ssh_public_key_file=ssh_public_key_path,
            add_on_operators=operator_names,
        )
        cluster = assert_cluster_with_two_node_sets_available(
            grpc=grpc, k8s=k8s_hub_client, metering=metering, uuid=uuid, instance_types=instance_types
        )
        if os.environ.get("OSAC_FULFILLMENT_TRUST_E2E") == "true":
            assert_management_tls(k8s_hub_client, require_metering=True)
            assert_cluster_trust(k8s_hub_client, cluster.co_name)

        wait_for_cluster_order_condition(k8s=k8s_hub_client, name=cluster.co_name, condition_type="AddOnOperatorsReady")
        hosted_cluster_name, hosted_cluster_namespace = _wait_for_hosted_cluster_references(
            k8s=k8s_hub_client, cluster_order_name=cluster.co_name
        )
        workload_kubeconfig = wait_for_hosted_cluster_kubeconfig(
            k8s=k8s_hub_client,
            hosted_cluster_namespace=hosted_cluster_namespace,
            hosted_cluster_name=hosted_cluster_name,
        )
        with tempfile.NamedTemporaryFile(prefix="osac-workload-", suffix=".kubeconfig") as kubeconfig_file:
            kubeconfig_file.write(workload_kubeconfig)
            kubeconfig_file.flush()
            workload_k8s = K8sClient(
                namespace=hosted_cluster_namespace, kubeconfig=kubeconfig_file.name, as_system_admin=False
            )
            _assert_subscriptions_and_csvs(
                workload_k8s=workload_k8s, hosted_cluster_name=hosted_cluster_name, expected=operator_specs
            )
        _wait_for_api_operators_installed(
            client=grpc,
            cluster_id=uuid,
            identity="CaaS two-node-set cluster",
            operator_names=(spec.name for spec in operator_specs),
        )
        cli.delete_cluster(uuid=uuid)
        _assert_cluster_deleted(grpc=grpc, k8s=k8s_hub_client, metering=metering, uuid=uuid, cluster=cluster)
    finally:
        with contextlib.suppress(subprocess.SubprocessError):
            if uuid:
                cli.delete_cluster(uuid=uuid)
        _restore_operator_publication(private_grpc, publication_states)
