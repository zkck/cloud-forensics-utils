# -*- coding: utf-8 -*-
# Copyright 2020 Google Inc.
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
"""Kubernetes workload classes extending the base hierarchy."""

import abc
from typing import List, Dict, Union

from kubernetes.client import (
  AppsV1Api,
  CoreV1Api,
  # Types
  V1Deployment,
  V1ReplicaSet,
)

from libcloudforensics.providers.kubernetes.base import (
  K8sNamespacedResource,
  K8sPod,
)
from libcloudforensics.providers.kubernetes.selector import K8sSelector


class K8sWorkload(K8sNamespacedResource, metaclass=abc.ABCMeta):
  """Abstract class representing Kubernetes workloads.

  A Kubernetes workload could be a ReplicaSet, a Deployment, a StatefulSet.
  """

  K8sWorkloadType = Union[
    V1Deployment,
    V1ReplicaSet,
  ]

  @abc.abstractmethod
  def PodMatchLabels(self) -> Dict[str, str]:
    """Gets the key-value pairs that pods belonging to this workload would have.

    Returns:
      Dict[str, str]: The label key-value pairs of this workload's pods.
    """

  @abc.abstractmethod
  def Read(self) -> 'K8sWorkloadType':
    pass  # Narrow down type hint

  def MatchLabels(self) -> Dict[str, str]:
    """Gets the label key-value pairs in the matchLabels field.

    Returns:
      Dict[str, str]: The label key-value pairs in the matchLabels field.

    Raises:
      NotImplementedError: If matchExpressions exist, meaning using matchLabels
        on their own would be inaccurate.
    """
    read = self.Read()
    # Get the selectors for this deployment
    if read.spec.selector.match_expressions is not None:
      raise NotImplementedError('matchExpressions exist, meaning using '
                                'matchLabels will be inaccurate.')
    # Extract with necessary type annotation
    match_labels: Dict[str, str] = read.spec.selector.match_labels
    return match_labels

  def GetCoveredPods(self) -> List[K8sPod]:
    """Gets a list of Kubernetes pods covered by this workload.

    Returns:
      List[K8sPod]: A list of pods covered by this workload.
    """
    api = self._Api(CoreV1Api)

    # Get the labels for this workload, and create a selector
    selector = K8sSelector.FromLabelsDict(self.PodMatchLabels())

    # Extract the pods
    pods = api.list_namespaced_pod(
      self.namespace,
      **selector.ToKeywords()
    )

    # Convert to pod objects
    return [K8sPod(self._api_client, pod.metadata.name, pod.metadata.namespace)
            for pod in pods.items]


class K8sDeployment(K8sWorkload):
  """Class representing a Kubernetes deployment."""

  def Read(self) -> V1Deployment:
    api = self._Api(AppsV1Api)
    return api.read_namespaced_deployment(self.name, self.namespace)

  def _ReplicaSet(self) -> 'K8sReplicaSet':
    """Gets the matching ReplicaSet of this deployment.

    Returns:
      K8sReplicaSet: The matching ReplicaSet of this deployment.
    """
    # Find the matching ReplicaSets, based on this deployment's matchLabels
    replica_sets_selector = K8sSelector.FromLabelsDict(self.MatchLabels())
    replica_sets = self._Api(AppsV1Api).list_namespaced_replica_set(
      self.namespace,
      **replica_sets_selector.ToKeywords()
    ).items

    # Check number of returned ReplicaSets
    if len(replica_sets) != 1:
      raise NotImplementedError(
        'Unexpected number of matching replica sets: '
        '{0:d} matching, expected 1.'.format(
          len(replica_sets)))

    return K8sReplicaSet(
      self._api_client,
      replica_sets[0].metadata.name,
      replica_sets[0].metadata.namespace,
    )

  def PodMatchLabels(self) -> Dict[str, str]:
    return self._ReplicaSet().MatchLabels()

class K8sReplicaSet(K8sWorkload):
  """Class representing a Kubernetes deployment."""

  def PodMatchLabels(self) -> Dict[str, str]:
    return self.MatchLabels()

  def Read(self) -> V1Deployment:
    api = self._Api(AppsV1Api)
    return api.read_namespaced_replica_set(self.name, self.namespace)
