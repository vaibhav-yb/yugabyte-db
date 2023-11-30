/*
 * Copyright (c) YugaByte, Inc.
 */

package universe

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yugabyte/yugabyte-db/managed/yba-cli/cmd/util"
	ybaAuthClient "github.com/yugabyte/yugabyte-db/managed/yba-cli/internal/client"
	"github.com/yugabyte/yugabyte-db/managed/yba-cli/internal/formatter"
)

// deleteUniverseCmd represents the universe command
var deleteUniverseCmd = &cobra.Command{
	Use:   "delete [universe-name]",
	Short: "Delete a YugabyteDB Anywhere universe",
	Long:  "Delete a universe in YugabyteDB Anywhere",
	Args:  cobra.MaximumNArgs(1),
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag("force", cmd.Flags().Lookup("force"))
		universeNameFlag, _ := cmd.Flags().GetString("name")
		var universeName string
		if len(args) > 0 {
			universeName = args[0]
		} else if len(universeNameFlag) > 0 {
			universeName = universeNameFlag
		} else {
			cmd.Help()
			logrus.Fatalln(
				formatter.Colorize("No universe name found to delete\n", formatter.RedColor))
		}
		err := util.ConfirmCommand(
			fmt.Sprintf("Are you sure you want to delete %s: %s", "universe", universeName),
			viper.GetBool("force"))
		if err != nil {
			logrus.Fatal(formatter.Colorize(err.Error(), formatter.RedColor))
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		authAPI, err := ybaAuthClient.NewAuthAPIClient()
		if err != nil {
			logrus.Fatalf(formatter.Colorize(err.Error()+"\n", formatter.RedColor))
		}
		authAPI.GetCustomerUUID()
		universeNameFlag, _ := cmd.Flags().GetString("name")
		var universeName string
		if len(args) > 0 {
			universeName = args[0]
		} else if len(universeNameFlag) > 0 {
			universeName = universeNameFlag
		}
		universeListRequest := authAPI.ListUniverses()
		universeListRequest = universeListRequest.Name(universeName)

		r, response, err := universeListRequest.Execute()
		if err != nil {
			errMessage := util.ErrorFromHTTPResponse(response, err, "Universe", "Delete")
			logrus.Fatalf(formatter.Colorize(errMessage.Error()+"\n", formatter.RedColor))
		}

		if len(r) < 1 {
			fmt.Println("No universes found")
			return
		}

		var universeUUID string
		if len(r) > 0 {
			universeUUID = r[0].GetUniverseUUID()
		}

		deleteUniverseRequest := authAPI.DeleteUniverse(universeUUID)

		forceDelete, err := cmd.Flags().GetBool("force-delete")
		if err != nil {
			logrus.Fatalf(formatter.Colorize(err.Error()+"\n", formatter.RedColor))
		}

		deleteBackups, err := cmd.Flags().GetBool("delete-backups")
		if err != nil {
			logrus.Fatalf(formatter.Colorize(err.Error()+"\n", formatter.RedColor))
		}
		deleteCerts, err := cmd.Flags().GetBool("delete-certs")
		if err != nil {
			logrus.Fatalf(formatter.Colorize(err.Error()+"\n", formatter.RedColor))
		}

		deleteUniverseRequest = deleteUniverseRequest.IsForceDelete(forceDelete).
			IsDeleteBackups(deleteBackups).IsDeleteAssociatedCerts(deleteCerts)

		rDelete, response, err := deleteUniverseRequest.Execute()
		if err != nil {
			errMessage := util.ErrorFromHTTPResponse(response, err, "Universe", "Delete")
			logrus.Fatalf(formatter.Colorize(errMessage.Error()+"\n", formatter.RedColor))
		}

		msg := fmt.Sprintf("The universe %s is being deleted",
			formatter.Colorize(universeName, formatter.GreenColor))

		if viper.GetBool("wait") {
			if rDelete.TaskUUID != nil {
				logrus.Info(fmt.Sprintf("Waiting for universe %s (%s) to be deleted\n",
					formatter.Colorize(universeName, formatter.GreenColor), universeUUID))
				err = authAPI.WaitForTask(*rDelete.TaskUUID, msg)
				if err != nil {
					logrus.Fatalf(formatter.Colorize(err.Error()+"\n", formatter.RedColor))
				}
			}
			fmt.Printf("The universe %s (%s) has been deleted\n",
				formatter.Colorize(universeName, formatter.GreenColor), universeUUID)
			return
		}
		fmt.Println(msg)
	},
}

func init() {
	deleteUniverseCmd.Flags().SortFlags = false
	deleteUniverseCmd.Flags().StringP("name", "n", "",
		"[Optional] The name of the universe to be deleted.")
	deleteUniverseCmd.Flags().BoolP("force", "f", false,
		"[Optional] Bypass the prompt for non-interactive usage.")
	deleteUniverseCmd.Flags().Bool("force-delete", false,
		"[Optional] Force delete the universe despite errors, defaults to false.")
	deleteUniverseCmd.Flags().Bool("delete-backups", false,
		"[Optional] Delete backups associated with universe-name, defaults to false.")
	deleteUniverseCmd.Flags().Bool("delete-certs", false,
		"[Optional] Delete certs associated with universe-name, defaults to false.")
}
