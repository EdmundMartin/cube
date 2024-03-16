/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"cube/worker"
	"fmt"
	"github.com/google/uuid"
	"log"

	"github.com/spf13/cobra"
)

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "worker",
	Long: `cube worker command.

The worker runs tasks and responds to the manager's requests about task
state.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		name, _ := cmd.Flags().GetString("name")
		dbType, _ := cmd.Flags().GetString("dbtype")

		log.Println("Starting worker.")
		w := worker.New(name, dbType)
		api := worker.Api{Address: host, Port: port, Worker: w}
		go w.RunTask()
		go w.UpdateTasks()
		log.Printf("Starting worker API on http://%s;%d", host, api.Start)
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)

	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().IntP("post", "p", 5556, "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "name of the worker")
	workerCmd.Flags().StringP("dbtype", "d", "memory", "Type of database to use")
}
