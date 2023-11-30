/*
 * Copyright (c) YugaByte, Inc.
 */

package universe

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yugabyte/yugabyte-db/managed/yba-cli/cmd/util"
	ybaAuthClient "github.com/yugabyte/yugabyte-db/managed/yba-cli/internal/client"
	"github.com/yugabyte/yugabyte-db/managed/yba-cli/internal/formatter"
	"github.com/yugabyte/yugabyte-db/managed/yba-cli/internal/formatter/universe"
)

var listUniverseCmd = &cobra.Command{
	Use:   "list",
	Short: "List YugabyteDB Anywhere universes",
	Long:  "List YugabyteDB Anywhere universes",
	Run: func(cmd *cobra.Command, args []string) {
		authAPI, err := ybaAuthClient.NewAuthAPIClient()
		if err != nil {
			logrus.Fatalf(formatter.Colorize(err.Error()+"\n", formatter.RedColor))
		}
		authAPI.GetCustomerUUID()
		universeListRequest := authAPI.ListUniverses()
		// filter by name and/or by universe code
		universeName, _ := cmd.Flags().GetString("name")
		if universeName != "" {
			universeListRequest = universeListRequest.Name(universeName)
		}

		r, response, err := universeListRequest.Execute()
		if err != nil {
			errMessage := util.ErrorFromHTTPResponse(response, err, "Universe", "List")
			logrus.Fatalf(formatter.Colorize(errMessage.Error()+"\n", formatter.RedColor))
		}

		universeCtx := formatter.Context{
			Output: os.Stdout,
			Format: universe.NewUniverseFormat(viper.GetString("output")),
		}
		if len(r) < 1 {
			fmt.Println("No universes found")
			return
		}
		universe.Write(universeCtx, r)

	},
}

func init() {
	listUniverseCmd.Flags().SortFlags = false

	listUniverseCmd.Flags().StringP("name", "n", "", "[Optional] Name of the universe.")
}
