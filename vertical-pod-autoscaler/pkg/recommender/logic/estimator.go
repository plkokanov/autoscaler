/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logic

import (
	"encoding/json"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	metricsquality "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/metrics/quality"
)

const (
	targetEstimatorName     = "targetEstimator"
	lowerBoundEstimatorName = "lowerBoundEstimator"
	upperBoundEstimatorName = "upperBoundEstimator"
)

// TODO: Split the estimator to have a separate estimator object for CPU and memory.

// ResourceEstimator is a function from AggregateContainerState to
// model.Resources, e.g. a prediction of resources needed by a group of
// containers.
type ResourceEstimator interface {
	GetResourceEstimation(s *model.AggregateContainerState) model.Resources
	SetVpaKeyAndEstimatorName(vpaKey model.VpaID, estimatorName string)
}

// Implementation of ResourceEstimator that returns constant amount of
// resources. This can be used as by a fake recommender for test purposes.
type constEstimator struct {
	resources     model.Resources
	vpaKey        model.VpaID
	estimatorName string
}

// Simple implementation of the ResourceEstimator interface. It returns specific
// percentiles of CPU usage distribution and memory peaks distribution.
type percentileEstimator struct {
	cpuPercentile    float64
	memoryPercentile float64
	vpaKey           model.VpaID
	estimatorName    string
}

type marginEstimator struct {
	marginFraction float64
	baseEstimator  ResourceEstimator
	vpaKey         model.VpaID
	estimatorName  string
}

type minResourcesEstimator struct {
	minResources  model.Resources
	baseEstimator ResourceEstimator
	vpaKey        model.VpaID
	estimatorName string
}

type confidenceMultiplier struct {
	multiplier    float64
	exponent      float64
	baseEstimator ResourceEstimator
	vpaKey        model.VpaID
	estimatorName string
}

// NewConstEstimator returns a new constEstimator with given resources.
func NewConstEstimator(resources model.Resources) ResourceEstimator {
	return &constEstimator{resources: resources}
}

// NewPercentileEstimator returns a new percentileEstimator that uses provided percentiles.
func NewPercentileEstimator(cpuPercentile float64, memoryPercentile float64) ResourceEstimator {
	return &percentileEstimator{cpuPercentile, memoryPercentile, model.VpaID{}, ""}
}

// WithMargin returns a given ResourceEstimator with margin applied.
// The returned resources are equal to the original resources plus (originalResource * marginFraction)
func WithMargin(marginFraction float64, baseEstimator ResourceEstimator) ResourceEstimator {
	return &marginEstimator{marginFraction, baseEstimator, model.VpaID{}, ""}
}

// WithMinResources returns a given ResourceEstimator with minResources applied.
// The returned resources are equal to the max(original resources, minResources)
func WithMinResources(minResources model.Resources, baseEstimator ResourceEstimator) ResourceEstimator {
	return &minResourcesEstimator{minResources, baseEstimator, model.VpaID{}, ""}
}

// WithConfidenceMultiplier returns a given ResourceEstimator with confidenceMultiplier applied.
func WithConfidenceMultiplier(multiplier, exponent float64, baseEstimator ResourceEstimator) ResourceEstimator {
	return &confidenceMultiplier{multiplier, exponent, baseEstimator, model.VpaID{}, ""}
}

// Returns a constant amount of resources.
func (e *constEstimator) GetResourceEstimation(s *model.AggregateContainerState) model.Resources {
	return e.resources
}

func (e *constEstimator) SetVpaKeyAndEstimatorName(vpaKey model.VpaID, estimatorName string) {
	e.vpaKey = vpaKey
	e.estimatorName = estimatorName
}

// Returns specific percentiles of CPU and memory peaks distributions.
func (e *percentileEstimator) GetResourceEstimation(s *model.AggregateContainerState) model.Resources {
	resources := model.Resources{
		model.ResourceCPU: model.CPUAmountFromCores(
			s.AggregateCPUUsage.Percentile(e.cpuPercentile)),
		model.ResourceMemory: model.MemoryAmountFromBytes(
			s.AggregateMemoryPeaks.Percentile(e.memoryPercentile)),
	}

	if resources["memory"] == 0 && e.estimatorName == targetEstimatorName {
		klog.Warningf("Computed %q resources for VPA %q were 0 in percentile estimator of %q!", "memory", klog.KRef(e.vpaKey.Namespace, e.vpaKey.VpaName), e.estimatorName)
	}

	return resources
}

func (e *percentileEstimator) SetVpaKeyAndEstimatorName(vpaKey model.VpaID, estimatorName string) {
	e.vpaKey = vpaKey
	e.estimatorName = estimatorName
}

// Returns a non-negative real number that heuristically measures how much
// confidence the history aggregated in the AggregateContainerState provides.
// For a workload producing a steady stream of samples over N days at the rate
// of 1 sample per minute, this metric is equal to N.
// This implementation is a very simple heuristic which looks at the total count
// of samples and the time between the first and the last sample.
func getConfidence(s *model.AggregateContainerState) float64 {
	// Distance between the first and the last observed sample time, measured in days.
	lifespanInDays := float64(s.LastSampleStart.Sub(s.FirstSampleStart)) / float64(time.Hour*24)
	// Total count of samples normalized such that it equals the number of days for
	// frequency of 1 sample/minute.
	samplesAmount := float64(s.TotalSamplesCount) / (60 * 24)
	return math.Min(lifespanInDays, samplesAmount)
}

// Returns resources computed by the underlying estimator, scaled based on the
// confidence metric, which depends on the amount of available historical data.
// Each resource is transformed as follows:
//
//	scaledResource = originalResource * (1 + 1/confidence)^exponent.
//
// This can be used to widen or narrow the gap between the lower and upper bound
// estimators depending on how much input data is available to the estimators.
func (e *confidenceMultiplier) GetResourceEstimation(s *model.AggregateContainerState) model.Resources {
	confidence := getConfidence(s)
	originalResources := e.baseEstimator.GetResourceEstimation(s)
	scaledResources := make(model.Resources)
	for resource, resourceAmount := range originalResources {
		scaledResources[resource] = model.ScaleResource(
			resourceAmount, math.Pow(1.+e.multiplier/confidence, e.exponent))
		if resource == "memory" && scaledResources[resource] == 0 && e.estimatorName == targetEstimatorName {
			klog.Warningf("Computed %q resources for VPA %q were 0 after applying confidence: %f in confidence multiplier of %q; they were %v before that!", resource, klog.KRef(e.vpaKey.Namespace, e.vpaKey.VpaName), confidence, e.estimatorName, resourceAmount)
		}
	}
	return scaledResources
}

func (e *confidenceMultiplier) SetVpaKeyAndEstimatorName(vpaKey model.VpaID, estimatorName string) {
	e.vpaKey = vpaKey
	e.estimatorName = estimatorName
	e.baseEstimator.SetVpaKeyAndEstimatorName(vpaKey, estimatorName)
}

func (e *marginEstimator) GetResourceEstimation(s *model.AggregateContainerState) model.Resources {
	originalResources := e.baseEstimator.GetResourceEstimation(s)
	newResources := make(model.Resources)
	for resource, resourceAmount := range originalResources {
		margin := model.ScaleResource(resourceAmount, e.marginFraction)
		newResources[resource] = originalResources[resource] + margin
		if resource == "memory" && newResources[resource] == 0 && e.estimatorName == targetEstimatorName {
			klog.Warningf("Computed %q resources for VPA %q were 0 after applying margin in margin estimator of %q, they were %v before that", resource, klog.KRef(e.vpaKey.Namespace, e.vpaKey.VpaName), e.estimatorName, originalResources[resource])
		}
	}
	return newResources
}

func (e *marginEstimator) SetVpaKeyAndEstimatorName(vpaKey model.VpaID, estimatorName string) {
	e.vpaKey = vpaKey
	e.estimatorName = estimatorName
	e.baseEstimator.SetVpaKeyAndEstimatorName(vpaKey, estimatorName)
}

func (e *minResourcesEstimator) GetResourceEstimation(s *model.AggregateContainerState) model.Resources {
	originalResources := e.baseEstimator.GetResourceEstimation(s)
	newResources := make(model.Resources)
	for resource, resourceAmount := range originalResources {
		if resourceAmount < e.minResources[resource] {
			if resource == "memory" && resourceAmount == 0 && e.estimatorName == targetEstimatorName {
				klog.Warningf("Computed %q resources for VPA %q were below minimum in min resource estimator of %q! Computed %v, minimum is %v.", resource, klog.KRef(e.vpaKey.Namespace, e.vpaKey.VpaName), e.estimatorName, resourceAmount, e.minResources[resource])
				logHistogramInformation(s, e.vpaKey)
				metricsquality.ObserveLowerThanMinRecommendation(s.GetUpdateMode(), corev1.ResourceName("memory"), e.vpaKey.Namespace+"/"+e.vpaKey.VpaName)
			}
			resourceAmount = e.minResources[resource]
		}
		newResources[resource] = resourceAmount
	}
	return newResources
}

func (e *minResourcesEstimator) SetVpaKeyAndEstimatorName(vpaKey model.VpaID, estimatorName string) {
	e.vpaKey = vpaKey
	e.estimatorName = estimatorName
	e.baseEstimator.SetVpaKeyAndEstimatorName(vpaKey, estimatorName)
}

func logHistogramInformation(s *model.AggregateContainerState, vpaKey model.VpaID) {
	if s.AggregateCPUUsage == nil {
		klog.Warning("Aggregate CPU usage has no metric samples, cannot show internal histogram data for VPA %q!", klog.KRef(vpaKey.Namespace, vpaKey.VpaName))
		return
	}
	if s.AggregateMemoryPeaks == nil {
		klog.Warning("Aggregate memory usage has no metric samples, cannot show internal histogram data for VPA %q!", klog.KRef(vpaKey.Namespace, vpaKey.VpaName))
		return
	}

	if s.AggregateMemoryPeaks.IsEmpty() {
		klog.Warningf("The memory histogram for VPA %q is empty!", klog.KRef(vpaKey.Namespace, vpaKey.VpaName))
	}

	klog.Warningf("Here's the string representation of the memory histogram for VPA %q: %s", klog.KRef(vpaKey.Namespace, vpaKey.VpaName), s.AggregateMemoryPeaks.String())

	c, _ := s.SaveToCheckpoint()
	prettyCheckpoint, err := json.Marshal(c)
	if err != nil {
		klog.Errorf("Error during marshalling checkpoint for VPA %q: %s", klog.KRef(vpaKey.Namespace, vpaKey.VpaName), err)
		return
	}
	klog.Warningf("Here's the checkpoint/state for VPA %q: %s", klog.KRef(vpaKey.Namespace, vpaKey.VpaName), prettyCheckpoint)
}
