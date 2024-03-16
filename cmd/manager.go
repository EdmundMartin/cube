/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"cube/manager"
	"github.com/spf13/cobra"
	"log"
)

// managerCmd represents the manager command
var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Manager command to operate a Cube manager node",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		workers, _ := cmd.Flags().GetStringSlice("workers")
		scheduler, _ := cmd.Flags().GetString("scheduler")
		dbType, _ := cmd.Flags().GetString("dbType")

		log.Println("Starting manager.")
		m := manager.New(workers, scheduler, dbType)
		api := manager.Api{Address: host, Port: port, Manager: m}
		go m.ProcessTasks()
		go m.UpdateTasks()
		go m.DoHealthChecks()
		log.Printf("Starting manager API on http://%s:%d", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)
	managerCmd.Flags().StringP("host", "H", "0.0.0.0",
		"Hostname or IP address")
	managerCmd.Flags().IntP("port", "p", 5555, "Port on which to listen")
	managerCmd.Flags().StringSliceP("workers", "w",
		[]string{"localhost:5556"}, "List of workers on which the manger will schedule tasks.")
	managerCmd.Flags().StringP("scheduler", "s", "roundrobin", "Nameof scheduler to use.")
	managerCmd.Flags().StringP("dbType", "d", "memory", "Type of datastore to use for events and tasks (\"memory\" or \"persistent\")")

}
