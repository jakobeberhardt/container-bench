package scheduler

import (
	"container-bench/internal/config"

	"github.com/sirupsen/logrus"
)

func containerLogFields(containers []ContainerInfo, containerIndex int, cfg *config.ContainerConfig) logrus.Fields {
	fields := logrus.Fields{"container_index": containerIndex}
	if cfg != nil {
		if cfg.Name != "" {
			fields["container_name"] = cfg.Name
		}
		if cfg.KeyName != "" {
			fields["container_key"] = cfg.KeyName
		}
		if cfg.Image != "" {
			fields["image"] = cfg.Image
		}
	}
	for i := range containers {
		if containers[i].Index != containerIndex {
			continue
		}
		if containers[i].ContainerID != "" {
			fields["container_id"] = containers[i].ContainerID
		}
		if containers[i].PID != 0 {
			fields["pid"] = containers[i].PID
		}
		break
	}
	return fields
}
