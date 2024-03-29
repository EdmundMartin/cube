package main

import (
	"cube/manager"
	"cube/worker"
	"fmt"
	"os"
	"strconv"
)

func main() {
	whost := GetEnvDefault("CUBE_WORKER_HOST", "localhost")
	wport, _ := strconv.Atoi(GetEnvDefault("CUBE_WORKER_PORT", "8080"))

	mhost := GetEnvDefault("CUBE_MANAGER_HOST", "localhost")
	mport, _ := strconv.Atoi(GetEnvDefault("CUBE_MANAGER_PORT", "8085"))

	fmt.Println("Starting Cube worker")

	w1 := worker.New("worker-1", "memory")
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: w1}

	w2 := worker.New("worker-2", "memory")
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: w2}

	w3 := worker.New("worker-3", "memory")
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: w3}

	go w1.RunTasks()
	go w1.UpdateTasks()
	go wapi1.Start()

	go w2.RunTasks()
	go w2.UpdateTasks()
	go wapi2.Start()

	go w3.RunTasks()
	go w3.UpdateTasks()
	go wapi3.Start()

	fmt.Println("Starting Cube manager")

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}

	m := manager.New(workers, "roundrobin", "memory")
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()
}

func GetEnvDefault(key, defaultVal string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	return defaultVal
}
