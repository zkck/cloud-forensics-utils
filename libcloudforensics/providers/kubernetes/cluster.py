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
"""Kubernetes cluster class."""
from typing import Optional, List

from libcloudforensics.providers.kubernetes import base, workloads
from kubernetes import client


class K8sCluster(base.K8sClient):
  """Class representing a Kubernetes cluster."""

  def ListPods(self, namespace: Optional[str] = None) -> List[base.K8sPod]:
    """Lists the pods of this cluster, possibly filtering for a namespace.

    Args:
      namespace (str): Optional. The namespace in which to list the pods.

    Returns:
      List[K8sPod]: The list of pods for the namespace, or in all namespaces
        if none is specified.
    """
    api = self._Api(client.CoreV1Api)

    # Collect pods
    if namespace is not None:
      pods = api.list_namespaced_pod(namespace)
    else:
      pods = api.list_pod_for_all_namespaces()

    # Convert to node objects
    return [
      base.K8sPod(self._api_client, pod.metadata.name, pod.metadata.namespace)
      for pod in pods.items]

  def ListNodes(self) -> List[base.K8sNode]:
    """Lists the nodes of this cluster.

    Returns:
      List[base.K8sNode]: The list of nodes in this cluster.
    """
    api = self._Api(client.CoreV1Api)

    # Collect pods
    nodes = api.list_node()

    # Convert to node objects
    return [base.K8sNode(self._api_client, node.metadata.name)
            for node in nodes.items]

  def GetDeployment(self, workload_id: str, namespace: str) -> workloads.K8sDeployment:
    """Gets the a deployment of this cluster.

    Returns:
      List[base.K8sNode]: The list of nodes in this cluster.
    """
    return workloads.K8sDeployment(self._api_client, workload_id, namespace)
