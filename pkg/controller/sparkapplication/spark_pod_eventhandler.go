/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	crdlisters "github.com/kubeflow/spark-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/config"
	"github.com/kubeflow/spark-operator/pkg/util"
)

// sparkPodEventHandler monitors Spark executor pods and update the SparkApplication objects accordingly.
type sparkPodEventHandler struct {
	metrics           *sparkEventMetrics
	applicationLister crdlisters.SparkApplicationLister
	// call-back function to enqueue SparkApp key for processing.
	enqueueFunc func(appKey interface{})
}

// newSparkPodEventHandler creates a new sparkPodEventHandler instance.
func newSparkPodEventHandler(enqueueFunc func(appKey interface{}), lister crdlisters.SparkApplicationLister, metricsConfig *util.MetricConfig) *sparkPodEventHandler {
	monitor := &sparkPodEventHandler{
		enqueueFunc:       enqueueFunc,
		applicationLister: lister,
	}
	if metricsConfig != nil {
		monitor.metrics = newSparkEventMetrics(metricsConfig)
		monitor.metrics.registerMetrics()
	}
	return monitor
}

func (s *sparkPodEventHandler) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	glog.V(2).Infof("Pod %s added in namespace %s.", pod.GetName(), pod.GetNamespace())
	s.enqueueSparkAppForUpdate(pod)
	if s.metrics != nil {
		s.metrics.sparkPodAddedCount.Inc()
	}
}

func (s *sparkPodEventHandler) onPodUpdated(old, updated interface{}) {
	oldPod := old.(*apiv1.Pod)
	updatedPod := updated.(*apiv1.Pod)

	if updatedPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}
	glog.V(2).Infof("Pod %s updated in namespace %s.", updatedPod.GetName(), updatedPod.GetNamespace())
	s.enqueueSparkAppForUpdate(updatedPod)
	if s.metrics != nil {
		s.metrics.sparkPodUpdatedCount.Inc()
	}

}

func (s *sparkPodEventHandler) onPodDeleted(obj interface{}) {
	var deletedPod *apiv1.Pod

	switch obj.(type) {
	case *apiv1.Pod:
		deletedPod = obj.(*apiv1.Pod)
	case cache.DeletedFinalStateUnknown:
		deletedObj := obj.(cache.DeletedFinalStateUnknown).Obj
		deletedPod = deletedObj.(*apiv1.Pod)
	}

	if deletedPod == nil {
		return
	}
	glog.V(2).Infof("Pod %s deleted in namespace %s.", deletedPod.GetName(), deletedPod.GetNamespace())
	s.enqueueSparkAppForUpdate(deletedPod)
	if s.metrics != nil {
		s.metrics.sparkPodDeletedCount.Inc()
	}
}

func (s *sparkPodEventHandler) enqueueSparkAppForUpdate(pod *apiv1.Pod) {
	appName, exists := getAppName(pod)
	if !exists {
		return
	}

	if submissionID, exists := pod.Labels[config.SubmissionIDLabel]; exists {
		app, err := s.applicationLister.SparkApplications(pod.GetNamespace()).Get(appName)
		if err != nil || app.Status.SubmissionID != submissionID {
			return
		}
	}

	appKey := createMetaNamespaceKey(pod.GetNamespace(), appName)
	glog.V(2).Infof("Enqueuing SparkApplication %s for app update processing.", appKey)
	s.enqueueFunc(appKey)
}
