package store

import (
	"cube/task"
	"fmt"
)

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

func (i *InMemoryTaskStore) Put(key string, value interface{}) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("value %v is not a task.Task", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (interface{}, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist")
	}
	return t, nil
}

func (i *InMemoryTaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{Db: map[string]*task.Task{}}
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

func (i *InMemoryTaskEventStore) Put(key string, value interface{}) error {
	e, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("value %v is not a task.TaskEvent", e)
	}
	i.Db[key] = e
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (interface{}, error) {
	e, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task event with key %s does not exist", key)
	}
	return e, nil
}

func (i *InMemoryTaskEventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	for _, e := range i.Db {
		events = append(events, e)
	}
	return events, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{Db: map[string]*task.TaskEvent{}}
}
