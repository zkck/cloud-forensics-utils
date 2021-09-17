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
"""Test on netpol Kubernetes objects."""

import unittest
from unittest import mock

from libcloudforensics.providers.kubernetes import workloads
from tests.providers.kubernetes import k8s_mocks


class K8sWorkloadTest(unittest.TestCase):
  """Test Kubernetes NetworkPolicy API calls."""

  mock_match_labels = {
    'app': 'nginx-klzkdoho'
  }

  @mock.patch.object(workloads.K8sWorkload, '__abstractmethods__', set())
  @mock.patch.object(workloads.K8sWorkload, '_PodMatchLabels')
  def testPodIsCoveredByWorkloadSameLabels(self, mock_pod_match_lables):
    """Tests that a pod is indeed covered by a workload."""
    # Override abstract method
    mock_pod_match_lables.return_value = self.mock_match_labels

    mock_pod_response = k8s_mocks.MakeMockPod(labels=self.mock_match_labels)
    mock_pod = k8s_mocks.MakeMockK8sPod('name', 'namespace', mock_pod_response)

    workload = workloads.K8sWorkload(k8s_mocks.MOCK_API_CLIENT, 'name', 'namespace')

    self.assertTrue(workload.IsCoveringPod(mock_pod))

  @mock.patch.object(workloads.K8sWorkload, '__abstractmethods__', set())
  @mock.patch.object(workloads.K8sWorkload, '_PodMatchLabels')
  def testPodIsCoveredByWorkloadDifferentLabels(self, mock_pod_match_lables):
    """Tests that a pod is not covered by a workload with different labels."""
    # Override abstract method
    mock_pod_match_lables.return_value = self.mock_match_labels

    mock_pod_response = k8s_mocks.MakeMockPod(labels={})
    mock_pod = k8s_mocks.MakeMockK8sPod('name', 'namespace', mock_pod_response)

    workload = workloads.K8sWorkload(k8s_mocks.MOCK_API_CLIENT, 'name', 'namespace')

    self.assertFalse(workload.IsCoveringPod(mock_pod))

  @mock.patch.object(workloads.K8sWorkload, '__abstractmethods__', set())
  @mock.patch.object(workloads.K8sWorkload, '_PodMatchLabels')
  def testPodIsCoveredByWorkloadDifferentNamespace(self, mock_pod_match_lables):
    """Tests that a pod is not covered by a workload with different namespace."""
    # Override abstract method
    mock_pod_match_lables.return_value = self.mock_match_labels

    mock_pod_response = k8s_mocks.MakeMockPod(labels=self.mock_match_labels)
    mock_pod = k8s_mocks.MakeMockK8sPod('name', 'production', mock_pod_response)

    workload = workloads.K8sWorkload(k8s_mocks.MOCK_API_CLIENT, 'name', 'namespace')

    self.assertFalse(workload.IsCoveringPod(mock_pod))
