// Copyright (c) YugaByte, Inc.

package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"node-agent/model"
	"node-agent/util"
	"os"
	"os/exec"
	"sort"
	"strconv"

	"github.com/olekukonko/tablewriter"
	funk "github.com/thoas/go-funk"
)

type shellTask struct {
	name string //Name of the task
	cmd  string
	args []string
	done bool
}

func NewShellTask(name string, cmd string, args []string) *shellTask {
	return &shellTask{name: name, cmd: cmd, args: args}
}
func (s shellTask) TaskName() string {
	return s.name
}

// Runs the Shell Task.
func (s *shellTask) Process(ctx context.Context) (string, error) {
	util.FileLogger().Debugf("Starting the shell request - %s", s.name)
	shellCmd := exec.Command(s.cmd, s.args...)
	var out bytes.Buffer
	var errOut bytes.Buffer
	var output string
	shellCmd.Stdout = &out
	shellCmd.Stderr = &errOut
	util.FileLogger().Infof("Running command %s with args %v", s.cmd, s.args)
	err := shellCmd.Run()
	if err != nil {
		output = errOut.String()
		util.FileLogger().Errorf("Shell Run - %s task failed - %s", s.name, err.Error())
		util.FileLogger().Errorf("Shell command output %s", output)
	} else {
		output = out.String()
		util.FileLogger().Debugf("Shell Run - %s task successful", s.name)
		util.FileLogger().Debugf("Shell command output %s", output)
	}
	s.done = true

	return output, err
}

func (s shellTask) Done() bool {
	return s.done
}

type PreflightCheckHandler struct {
	provider     *model.Provider
	instanceType *model.NodeInstanceType
	accessKey    *model.AccessKey
	result       *map[string]model.PreflightCheckVal
}

func NewPreflightCheckHandler(
	provider *model.Provider,
	instanceType *model.NodeInstanceType,
	accessKey *model.AccessKey,
) *PreflightCheckHandler {
	return &PreflightCheckHandler{
		provider:     provider,
		instanceType: instanceType,
		accessKey:    accessKey,
	}
}

func (handler *PreflightCheckHandler) Handle(ctx context.Context) (any, error) {
	util.FileLogger().Debug("Starting Preflight checks handler.")
	var err error
	preflightScriptPath := util.PreflightCheckPath()
	shellCmdTask := NewShellTask(
		"runPreflightCheckScript",
		util.DefaultShell,
		handler.getOptions(preflightScriptPath),
	)
	output, err := shellCmdTask.Process(ctx)
	if err != nil {
		util.FileLogger().Errorf("Pre-flight checks processing failed - %s", err.Error())
		return nil, err
	}
	handler.result = &map[string]model.PreflightCheckVal{}
	err = json.Unmarshal([]byte(output), handler.result)
	if err != nil {
		util.FileLogger().Errorf("Pre-flight checks unmarshaling error - %s", err.Error())
		return nil, err
	}
	return handler.result, nil
}

func (handler *PreflightCheckHandler) Result() *map[string]model.PreflightCheckVal {
	return handler.result
}

// Returns options for the preflight checks.
func (handler *PreflightCheckHandler) getOptions(preflightScriptPath string) []string {
	options := make([]string, 3)
	options[0] = preflightScriptPath
	options[1] = "-t"
	options[2] = "provision"
	provider := handler.provider
	instanceType := handler.instanceType
	accessKey := handler.accessKey
	if provider.AirGapInstall {
		options = append(options, "--airgap")
	}
	//To-do: Should the api return a string instead of a list?
	if data := provider.CustomHostCidrs; len(data) > 0 {
		options = append(options, "--yb_home_dir", data[0])
	} else {
		options = append(options, "--yb_home_dir", util.NodeHomeDirectory)
	}

	if data := provider.SshPort; data != 0 {
		options = append(options, "--ports_to_check", fmt.Sprint(data))
	}

	if data := instanceType.Details.VolumeDetailsList; len(data) > 0 {
		options = append(options, "--mount_points")
		mp := ""
		for i, volumeDetail := range data {
			mp += volumeDetail.MountPath
			if i < len(data)-1 {
				mp += ","
			}
		}
		options = append(options, mp)
	}
	if accessKey.KeyInfo.InstallNodeExporter {
		options = append(options, "--install_node_exporter")
	}
	if accessKey.KeyInfo.AirGapInstall {
		options = append(options, "--airgap")
	}
	// TODO more options.
	return options
}

func HandleUpgradeScript(config *util.Config, ctx context.Context, version string) error {
	util.FileLogger().Debug("Initializing the upgrade script")
	upgradeScriptTask := NewShellTask(
		"upgradeScript",
		util.DefaultShell,
		[]string{util.UpgradeScriptPath(), "upgrade", version},
	)
	errStr, err := upgradeScriptTask.Process(ctx)
	if err != nil {
		return errors.New(errStr)
	}
	return nil
}

// Shell task process for downloading the node-agent build package.
func HandleDownloadPackageScript(config *util.Config, ctx context.Context) (string, error) {
	util.FileLogger().Debug("Initializing the download package script")
	jwtToken, err := util.GenerateJWT(config)
	if err != nil {
		util.FileLogger().Errorf("Failed to generate JWT during upgrade - %s", err.Error())
		return "", err
	}
	downloadPackageScript := NewShellTask(
		"downloadPackageScript",
		util.DefaultShell,
		[]string{
			util.InstallScriptPath(),
			"--type",
			"upgrade",
			"--url",
			config.String(util.PlatformUrlKey),
			"--jwt",
			jwtToken,
		},
	)
	return downloadPackageScript.Process(ctx)
}

func OutputPreflightCheck(responses map[string]model.NodeInstanceValidationResponse) bool {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Preflight Check", "Value", "Description", "Required", "Result"})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoWrapText(false)
	table.SetRowLine(true)
	table.SetHeaderColor(
		tablewriter.Colors{},
		tablewriter.Colors{},
		tablewriter.Colors{},
		tablewriter.Colors{},
		tablewriter.Colors{},
	)
	keys := funk.Keys(responses).([]string)
	sort.Strings(keys)
	allValid := true
	for _, k := range keys {
		v := responses[k]
		if v.Valid {
			data := []string{k, v.Value, v.Description, strconv.FormatBool(v.Required), "Passed"}
			table.Rich(
				data,
				[]tablewriter.Colors{
					tablewriter.Colors{tablewriter.FgGreenColor},
					tablewriter.Colors{tablewriter.FgGreenColor},
					tablewriter.Colors{tablewriter.FgGreenColor},
					tablewriter.Colors{tablewriter.FgGreenColor},
					tablewriter.Colors{tablewriter.FgGreenColor},
				},
			)
		} else {
			allValid = false
			data := []string{k, v.Value, v.Description, strconv.FormatBool(v.Required), "Failed"}
			table.Rich(data, []tablewriter.Colors{
				tablewriter.Colors{tablewriter.FgRedColor},
				tablewriter.Colors{tablewriter.FgRedColor},
				tablewriter.Colors{tablewriter.FgRedColor},
				tablewriter.Colors{tablewriter.FgRedColor},
				tablewriter.Colors{tablewriter.FgRedColor},
			})
		}
	}
	table.Render()
	return allValid
}
