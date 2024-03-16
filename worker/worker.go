package worker

import (
	"cube/store"
	"cube/task"
	"errors"
	"fmt"
	"github.com/golang-collections/collections/queue"
	"log"
	"time"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        store.Store
	TaskCount int
}

func New(name string, taskDbType string) *Worker {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
	}
	var s store.Store
	switch taskDbType {
	case "memory":
		s = store.NewInMemoryTaskStore()
	case "persistent":
		filename := fmt.Sprintf("%s_tasks.db", name)
		s, _ = store.NewTaskStore(filename, 0600, "tasks")
	}
	w.Db = s
	return &w
}

func (w *Worker) CollectStats() {

}

func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)
	loadedTask, _ := w.Db.Get(taskQueued.ID.String())

	taskPersisted := loadedTask.(*task.Task)

	if taskPersisted == nil {
		taskPersisted = &taskQueued
		err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
		if err != nil {
			msg := fmt.Errorf("error storing task %s: %v",
				taskQueued.ID.String(), err)
			log.Println(msg)
			return task.DockerResult{Error: msg}
		}
	}

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("unexpected task state")
		}
	} else {
		err := fmt.Errorf("invalid state transistion from %v to %v", taskPersisted.State, taskQueued.State)
		result.Error = err
	}
	return result
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		log.Printf("Err running task: %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db.Put(t.ID.String(), &t)
		return result
	}
	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db.Put(t.ID.String(), &t)
	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db.Put(t.ID.String(), &t)
	log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}

func (w *Worker) GetTasks() []*task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("error getting list of tasks: %v\n", err)
		return nil
	}
	return taskList.([]*task.Task)
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently")
		}
		log.Println("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("Checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed")
		log.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	taskResp, _ := w.Db.List()
	allTasks := taskResp.([]*task.Task)
	for id, t := range allTasks {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("No container for runningt task %s\n", id)
				t.State = task.Failed
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("Container for task %s in non-running state %s", t.ID, resp.Container.State.Status)
				t.State = task.Failed
			}
			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports

			w.Db.Put(t.ID.String(), t)
		}
	}
}
