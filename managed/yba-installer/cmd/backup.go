/*
 * Copyright (c) YugaByte, Inc.
 */

package cmd

import (
	"os"

	"github.com/spf13/viper"
	"github.com/yugabyte/yugabyte-db/managed/yba-installer/common"
	log "github.com/yugabyte/yugabyte-db/managed/yba-installer/logging"
)

// CreateBackupScript calls the yb_platform_backup.sh script with the correct args.
func CreateBackupScript(outputPath string, dataDir string,
	excludePrometheus bool, skipRestart bool, verbose bool, plat Platform) {

	fileName := plat.backupScript()
	err := os.Chmod(fileName, 0777)
	if err != nil {
		log.Fatal(err.Error())
	} else {
		log.Debug("Create Backup Script has now been given executable permissions.")
	}

	args := []string{"create", "--output", outputPath, "--data_dir", dataDir, "--yba_installer"}
	if excludePrometheus {
		args = append(args, "--exclude-prometheus")
	}
	if skipRestart {
		args = append(args, "--skip_restart")
	}
	if verbose {
		args = append(args, "--verbose")
	}

	if viper.GetBool("postgres.useExisting.enabled") {
		args = append(args, "--db_username", viper.GetString("postgres.useExisting.username"))
		args = append(args, "--db_host", viper.GetString("postgres.useExisting.host"))
		args = append(args, "--db_port", viper.GetString("postgres.useExisting.port"))
		// TODO: modify yb platform backup sript to accept a custom password
	}

	if viper.GetBool("postgres.install.enabled") {
		args = append(args, "--db_username", "postgres")
		args = append(args, "--db_host", "localhost")
		args = append(args, "--db_port", viper.GetString("postgres.install.port"))
	}

	log.Info("Creating a backup of your Yugabyte Anywhere Installation.")
	common.RunBash(fileName, args)
}

// RestoreBackupScript calls the yb_platform_backup.sh script with the correct args.
// TODO: Version check is still disabled because of issues finding the path across all installs.
func RestoreBackupScript(inputPath string, destination string, skipRestart bool,
	verbose bool, plat Platform) {
	userName := viper.GetString("service_username")
	fileName := plat.backupScript()
	err := os.Chmod(fileName, 0777)
	if err != nil {
		log.Fatal(err.Error())
	} else {
		log.Debug("Restore Backup Script has now been given executable permissions.")
	}

	args := []string{"restore", "--input", inputPath,
		"--destination", destination, "--data_dir", destination, "--disable_version_check",
		"--yba_installer"}
	if skipRestart {
		args = append(args, "--skip_restart")
	}
	if verbose {
		args = append(args, "--verbose")
	}
	if common.HasSudoAccess() {
		args = append(args, "-u", userName, "-e", userName)
	} else {
		args = append(args, "-u", common.GetCurrentUser(), "-e", common.GetCurrentUser())
	}
	log.Info("Restoring a backup of your Yugabyte Anywhere Installation.")
	common.RunBash(fileName, args)

}
