// Command hermit-scheduler is kube-scheduler with the HermitLocality plugin
// compiled in. Enable the plugin in a profile of its KubeSchedulerConfiguration
// (see deploy/scheduler/).
package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/tsuzu/hermit-csi/scheduler/pkg/locality"
)

func main() {
	cmd := app.NewSchedulerCommand(app.WithPlugin(locality.Name, locality.New))
	os.Exit(cli.Run(cmd))
}
