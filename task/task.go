package task

import (
	"context"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"io"
	"log"
	"math"
	"os"
	"time"
)

type Task struct {
	ID            uuid.UUID
	Name          string
	State         State
	Image         string
	Cpu           float64
	Memory        int
	Disk          int
	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
	RestartPolicy string
	StartTime     time.Time
	FinishTime    time.Time
	ContainerID   string
	HostPorts     nat.PortMap

	HealthCheck  string
	RestartCount int
}

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}

type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	ExposedPorts  nat.PortSet
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Env           []string
	RestartPolicy string
	Runtime       Runtime
}

type DockerInspectResponse struct {
	Error     error
	Container *types.ContainerJSON
}

func NewConfig(task *Task) Config {
	return Config{
		Name:          task.Name,
		AttachStdin:   true,
		AttachStdout:  true,
		AttachStderr:  true,
		ExposedPorts:  task.ExposedPorts,
		Cmd:           nil,
		Image:         task.Image,
		Cpu:           4,
		Memory:        1024,
		Env:           nil,
		RestartPolicy: task.RestartPolicy,
	}
}

type Runtime struct {
	ContainerID string
}

type Docker struct {
	Client *client.Client
	Config Config
}

func (d *Docker) Inspect(containerID string) DockerInspectResponse {
	dc, _ := client.NewClientWithOpts(client.FromEnv)
	ctx := context.Background()
	resp, err := dc.ContainerInspect(ctx, containerID)
	if err != nil {
		log.Printf("Error inspecting container: %s\n", containerID)
		return DockerInspectResponse{Error: err}
	}
	return DockerInspectResponse{Container: &resp}
}

func NewDocker(config Config) Docker {
	cli, _ := client.NewClientWithOpts(client.FromEnv)
	return Docker{
		Config: config,
		Client: cli,
	}
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(
		ctx, d.Config.Image, types.ImagePullOptions{},
	)
	if err != nil {
		log.Printf("Error pulling image: %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	io.Copy(os.Stdout, reader)

	rp := container.RestartPolicy{
		Name: container.RestartPolicyMode(d.Config.RestartPolicy),
	}

	r := container.Resources{
		Memory:   d.Config.Memory * 1000000,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}

	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	resp, err := d.Client.ContainerCreate(ctx, &cc, &hc, nil, nil, d.Config.Name)
	if err != nil {
		log.Printf("Error creating container using image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	err = d.Client.ContainerStart(ctx, resp.ID, container.StartOptions{})
	if err != nil {
		log.Printf("Error starting container: %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	d.Config.Runtime.ContainerID = resp.ID

	out, err := d.Client.ContainerLogs(
		ctx,
		resp.ID,
		container.LogsOptions{ShowStdout: true, ShowStderr: true},
	)
	if err != nil {
		log.Printf("Error getting logs for container: %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}
	stdcopy.StdCopy(os.Stdout, os.Stdout, out)

	return DockerResult{ContainerId: resp.ID, Action: "start", Result: "success"}
}

func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)
	ctx := context.Background()
	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		log.Printf("Error stopping container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	err = d.Client.ContainerRemove(ctx, id, container.RemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         false,
	})
	if err != nil {
		log.Printf("Error removing container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	return DockerResult{Action: "stop", Result: "success", Error: nil}
}
