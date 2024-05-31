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
	"github.com/kubeflow/spark-operator/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

type sparkEventMetrics struct {
	prefix               string
	sparkPodAddedCount   prometheus.Counter
	sparkPodUpdatedCount prometheus.Counter
	sparkPodDeletedCount prometheus.Counter
}

func newSparkEventMetrics(metricsConfig *util.MetricConfig) *sparkEventMetrics {
	prefix := metricsConfig.MetricsPrefix
	sparkPodAddedCount := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_pod_added_event_count"),
			Help: "Spark Pod Added Event Count by the Operator",
		},
	)
	sparkPodUpdatedCount := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_pod_updated_event_count"),
			Help: "Spark Pod Updated Event Count by the Operator",
		},
	)
	sparkPodDeletedCount := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_pod_deleted_event_count"),
			Help: "Spark Pod Deleted Event Count by the Operator",
		},
	)

	return &sparkEventMetrics{
		prefix:               prefix,
		sparkPodAddedCount:   sparkPodAddedCount,
		sparkPodUpdatedCount: sparkPodUpdatedCount,
		sparkPodDeletedCount: sparkPodDeletedCount,
	}
}

func (sm *sparkEventMetrics) registerMetrics() {
	util.RegisterMetric(sm.sparkPodAddedCount)
	util.RegisterMetric(sm.sparkPodUpdatedCount)
	util.RegisterMetric(sm.sparkPodDeletedCount)
}
