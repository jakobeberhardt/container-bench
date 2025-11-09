package kernels

import (
	"context"
	"time"

	"container-bench/internal/config"
	"container-bench/internal/dataframe"

	"github.com/docker/docker/client"
)

// all computed sensitivity metrics
// TODO reduce to minimum
type ProbeSensitivities struct {
	CPUInteger   *float64
	CPUFloat     *float64
	LLC          *float64
	MemRead      *float64
	MemWrite     *float64
	StoreBuffer  *float64
	Scoreboard   *float64
	NetworkRead  *float64
	NetworkWrite *float64
	SysCall      *float64

	// Dataframe range used for analysis
	FirstDataframeStep int
	LastDataframeStep  int
}

// ProbeKernel defines the interface for probe kernel implementations
// A probe kernel controls the probing sequence inside a running container
type ProbeKernel interface {
	GetName() string

	GetVersion() string

	// Runs the complete probing sequence inside the container
	// It has full control over what commands to run, when, and for how long
	// Returns computed sensitivities
	ExecuteProbe(
		ctx context.Context,
		dockerClient *client.Client,
		containerID string,
		totalTime time.Duration,
		cores string,
		dataframes *dataframe.DataFrames,
		targetContainerIndex int,
		targetContainerConfig *config.ContainerConfig,
	) (*ProbeSensitivities, error)
}
