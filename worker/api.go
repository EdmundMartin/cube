package worker

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"log"
	"net/http"
)

type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
}

func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}

type ErrorResponse struct {
	HTTPStatusCode int    `json:"http_status_code"`
	Message        string `json:"message"`
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		e := ErrorResponse{
			HTTPStatusCode: http.StatusBadRequest,
			Message:        msg,
		}
		_ = json.NewEncoder(w).Encode(e)
		return
	}
	a.Worker.AddTask(te.Task)
	log.Printf("Added tak %v\n", te.Task.ID)
	w.WriteHeader(201)
	_ = json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("No taskID passd in request.\n")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tID, _ := uuid.Parse(taskID)
	t, err := a.Worker.Db.Get(tID.String())
	if err != nil {
		log.Printf("No task with ID %v found", tID)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	taskToStop := t.(*task.Task)
	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	log.Printf("Added task %v to stop container %v\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(204)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}
