package dataparser

import (
	"sync"
	"time"

	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
)

// EnhancedSamplingStep extends SamplingStep with derived fields
type EnhancedSamplingStep struct {
	*dataframe.SamplingStep
	RelativeTime time.Duration `json:"relative_time"` // Time relative to benchmark start (0-based)
}

// EnhancedContainerDataFrame contains enhanced sampling steps
type EnhancedContainerDataFrame struct {
	steps map[int]*EnhancedSamplingStep
	mutex sync.RWMutex
}

// EnhancedDataFrames contains enhanced container data frames
type EnhancedDataFrames struct {
	containers map[int]*EnhancedContainerDataFrame
	mutex      sync.RWMutex
}

// ProcessDataFrames takes raw dataframes and adds derived columns
func ProcessDataFrames(rawDataframes *dataframe.DataFrames, benchmarkStartTime time.Time) (*EnhancedDataFrames, error) {
	logger := logging.GetLogger()
	logger.Info("Processing dataframes and adding derived columns")

	enhanced := &EnhancedDataFrames{
		containers: make(map[int]*EnhancedContainerDataFrame),
	}

	// Process each container
	containers := rawDataframes.GetAllContainers()
	for containerIndex, containerDF := range containers {
		enhancedContainer := &EnhancedContainerDataFrame{
			steps: make(map[int]*EnhancedSamplingStep),
		}

		// Process each sampling step
		steps := containerDF.GetAllSteps()
		for stepNumber, step := range steps {
			if step == nil {
				continue
			}

			// Calculate relative time from benchmark start
			relativeTime := step.Timestamp.Sub(benchmarkStartTime)

			// Create enhanced step with derived fields
			enhancedStep := &EnhancedSamplingStep{
				SamplingStep: step,
				RelativeTime: relativeTime,
			}

			enhancedContainer.steps[stepNumber] = enhancedStep
		}

		enhanced.containers[containerIndex] = enhancedContainer
	}

	logger.WithField("containers_processed", len(enhanced.containers)).Info("Dataframe processing completed")
	return enhanced, nil
}

// GetAllContainers returns all enhanced container dataframes
func (edf *EnhancedDataFrames) GetAllContainers() map[int]*EnhancedContainerDataFrame {
	edf.mutex.RLock()
	defer edf.mutex.RUnlock()
	
	result := make(map[int]*EnhancedContainerDataFrame)
	for k, v := range edf.containers {
		result[k] = v
	}
	return result
}

// GetAllSteps returns all enhanced sampling steps for a container
func (ecdf *EnhancedContainerDataFrame) GetAllSteps() map[int]*EnhancedSamplingStep {
	ecdf.mutex.RLock()
	defer ecdf.mutex.RUnlock()
	
	result := make(map[int]*EnhancedSamplingStep)
	for k, v := range ecdf.steps {
		result[k] = v
	}
	return result
}
