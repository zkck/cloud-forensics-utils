# -*- coding: utf-8 -*-
# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Mitigation functions to be used in end-to-end functionality."""

from libcloudforensics.providers.kubernetes import workloads, cluster


def DrainWorkloadNodesFromOtherPods(workload: workloads.K8sWorkload,
                                    cordon: bool = True) -> None:
  """Drains a workload's nodes from non-workload pods.

  Args:
    workload (workloads.K8sWorkload): The workload for which nodes
      must be drained from pods that are not covered by the workload.
    cordon (bool): Whether or not to cordon the nodes before draining, to
      prevent pods from appearing on the nodes again as it will be marked as
      unschedulable.
  """
  nodes = set(pod.GetNode() for pod in workload.GetCoveredPods())
  if cordon:
    for node in nodes:
      node.Cordon()
  for node in nodes:
    node.Drain(lambda p: not workload.IsCoveringPod(p))

def CreateDenyAllNetworkPolicyForWorkload(
    k8s_cluster: cluster.K8sCluster,
    workload: workloads.K8sWorkload
  ) -> None:
  """Isolates a workload's pods via a deny all network policy.

  Args:
    k8s_cluster (cluster.K8sCluster): The cluster in which to create the deny
      all policy, and subsequently patch existing policies
    workload (workloads.K8sWorkload): The workload in whose namespace the
      deny all network policy will be created, and whose pods will be tagged
      to be selected by the deny all network policy.
  """
  # TODO: Check that network policies are enabled
  # First create the NetworkPolicy in the workload's namespace
  deny_all_policy = k8s_cluster.DenyAllNetworkPolicy(workload.namespace)
  deny_all_policy.Create()
  # Tag the pods covered by the workload with the selecting label of the
  # deny all NetworkPolicy
  workload.AddTemplateLabels(deny_all_policy.labels)
  # For all other policies, specify that they are not selecting the pods
  # that are selected by the deny all policy
  # TODO: Patch other policies (in same namespace?)
