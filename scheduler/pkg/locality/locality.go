// Package locality is a kube-scheduler plugin that prefers nodes which
// already hold an up-to-date local copy of a pod's hermit-csi volumes.
//
// Staging a hermit volume on a node without a current local copy restores it
// from S3 first, which keeps the pod in ContainerCreating for as long as the
// download takes. Staging it on the node it was last attached to is instant.
// The driver records that node in volumes/<id>/owner.json (the current owner
// while attached, lastNode after a detach); this plugin reads it in PreScore
// and scores each node by the share of the pod's hermit capacity it holds.
//
// The plugin only scores: any node can still host any volume, so a missing or
// unreadable owner record just means "no preference".
package locality

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

// Name is the plugin name used in KubeSchedulerConfiguration.
const Name = "HermitLocality"

const stateKey fwk.StateKey = Name

// Args are the plugin arguments (pluginConfig[].args). The S3 settings must
// match the driver's. Empty S3 fields fall back to the environment variables
// of the driver's hermit-csi ConfigMap (S3_BUCKET, S3_PREFIX, S3_ENDPOINT,
// S3_REGION, S3_PATH_STYLE), so the scheduler can share it. Credentials come
// from the default AWS chain (env vars, IRSA, ...), as for the driver.
type Args struct {
	// DriverName of the CSI driver whose volumes are scored. Default hermit.csi.tsuzu.dev.
	DriverName string `json:"driverName,omitempty"`
	Bucket     string `json:"bucket"`
	Prefix     string `json:"prefix,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
	Region     string `json:"region,omitempty"`
	PathStyle  bool   `json:"pathStyle,omitempty"`
	// Timeout bounds the owner.json reads of one scheduling cycle, so a slow
	// S3 never stalls scheduling. Default 2s.
	Timeout string `json:"timeout,omitempty"`
}

// DefaultDriverName is the hermit-csi driver name.
const DefaultDriverName = "hermit.csi.tsuzu.dev"

func parseArgs(obj runtime.Object) (Args, time.Duration, error) {
	var a Args
	if err := frameworkruntime.DecodeInto(obj, &a); err != nil {
		return a, 0, fmt.Errorf("%s args: %w", Name, err)
	}
	if a.DriverName == "" {
		a.DriverName = DefaultDriverName
	}
	timeout := 2 * time.Second
	if a.Timeout != "" {
		d, err := time.ParseDuration(a.Timeout)
		if err != nil || d <= 0 {
			return a, 0, fmt.Errorf("%s args: invalid timeout %q", Name, a.Timeout)
		}
		timeout = d
	}
	return a, timeout, nil
}

// Plugin implements PreScore and Score.
type Plugin struct {
	driver  string
	timeout time.Duration
	pvcs    corelisters.PersistentVolumeClaimLister
	pvs     corelisters.PersistentVolumeLister
	s       store.ObjectStore
}

var (
	_ fwk.PreScorePlugin = (*Plugin)(nil)
	_ fwk.ScorePlugin    = (*Plugin)(nil)
	_ fwk.SignPlugin     = (*Plugin)(nil)
)

// New is the PluginFactory registered with the scheduler; it connects to S3.
func New(ctx context.Context, obj runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	a, _, err := parseArgs(obj)
	if err != nil {
		return nil, err
	}
	cfg := storeConfig(a, os.Getenv)
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("%s args: bucket is required (args.bucket or S3_BUCKET)", Name)
	}
	s, err := store.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return NewWithStore(s)(ctx, obj, h)
}

// storeConfig fills empty S3 args from the driver's environment variables.
func storeConfig(a Args, getenv func(string) string) store.Config {
	or := func(v, env string) string {
		if v != "" {
			return v
		}
		return getenv(env)
	}
	c := store.Config{
		Bucket:    or(a.Bucket, "S3_BUCKET"),
		Prefix:    or(a.Prefix, "S3_PREFIX"),
		Endpoint:  or(a.Endpoint, "S3_ENDPOINT"),
		Region:    or(a.Region, "S3_REGION"),
		PathStyle: a.PathStyle,
	}
	if !c.PathStyle {
		c.PathStyle, _ = strconv.ParseBool(getenv("S3_PATH_STYLE"))
	}
	if c.Region == "" {
		c.Region = "us-east-1" // same default as the driver
	}
	c.ApplyRoleEnv(getenv) // same role settings as the driver
	return c
}

// NewWithStore returns a PluginFactory that reads owner records from s
// instead of connecting to S3 (tests, or embedding with a custom store).
func NewWithStore(s store.ObjectStore) frameworkruntime.PluginFactory {
	return func(_ context.Context, obj runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
		a, timeout, err := parseArgs(obj)
		if err != nil {
			return nil, err
		}
		core := h.SharedInformerFactory().Core().V1()
		return &Plugin{
			driver:  a.DriverName,
			timeout: timeout,
			pvcs:    core.PersistentVolumeClaims().Lister(),
			pvs:     core.PersistentVolumes().Lister(),
			s:       s,
		}, nil
	}
}

func (p *Plugin) Name() string { return Name }

// SignPod lets pods with the same volumes share scheduling results, like the
// VolumeBinding plugin; the score depends only on those volumes.
func (p *Plugin) SignPod(_ context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{{Key: fwk.VolumesSignerName, Value: fwk.VolumesSigner(pod)}}, nil
}

// volume is one bound hermit volume of the pod.
type volume struct {
	id     string // CSI volume handle
	weight int64  // PV capacity in bytes (restore time grows with size)
}

// hermitVolumes returns the pod's bound PVCs that are hermit volumes.
// Unbound claims (WaitForFirstConsumer) are new volumes with no local copy.
func (p *Plugin) hermitVolumes(pod *v1.Pod) ([]volume, error) {
	var out []volume
	for _, v := range pod.Spec.Volumes {
		var claim string
		switch {
		case v.PersistentVolumeClaim != nil:
			claim = v.PersistentVolumeClaim.ClaimName
		case v.Ephemeral != nil:
			claim = pod.Name + "-" + v.Name // generic ephemeral volume naming
		default:
			continue
		}
		pvc, err := p.pvcs.PersistentVolumeClaims(pod.Namespace).Get(claim)
		if apierrors.IsNotFound(err) {
			continue // VolumeBinding reports missing claims
		}
		if err != nil {
			return nil, err
		}
		if pvc.Spec.VolumeName == "" {
			continue
		}
		pv, err := p.pvs.Get(pvc.Spec.VolumeName)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != p.driver {
			continue
		}
		w := int64(1)
		if q, ok := pv.Spec.Capacity[v1.ResourceStorage]; ok && q.Value() > 0 {
			w = q.Value()
		}
		if !slices.ContainsFunc(out, func(o volume) bool { return o.id == pv.Spec.CSI.VolumeHandle }) {
			out = append(out, volume{id: pv.Spec.CSI.VolumeHandle, weight: w})
		}
	}
	return out, nil
}

type preScoreState struct {
	byNode map[string]int64 // node -> capacity of the pod's volumes it holds
	total  int64            // capacity of all the pod's hermit volumes
}

func (s *preScoreState) Clone() fwk.StateData { return s } // read-only after PreScore

// PreScore resolves the pod's hermit volumes and reads their owner records.
// It returns Skip (so Score is not called) when no node holds any of them.
func (p *Plugin) PreScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)
	vols, err := p.hermitVolumes(pod)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if len(vols) == 0 {
		return fwk.NewStatus(fwk.Skip)
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	nodes := make([]string, len(vols))
	var wg sync.WaitGroup
	for i, v := range vols {
		wg.Go(func() {
			o, _, err := manifest.LoadOwner(ctx, p.s, v.id)
			switch {
			case errors.Is(err, store.ErrNotFound):
				// Not created yet / deleted: no preference.
			case err != nil:
				logger.Error(err, "read owner record, ignoring volume for locality", "pod", klog.KObj(pod), "volume", v.id)
			default:
				nodes[i] = o.LocalCopyNode()
			}
		})
	}
	wg.Wait()

	st := &preScoreState{byNode: map[string]int64{}}
	for i, v := range vols {
		st.total += v.weight
		if nodes[i] != "" {
			st.byNode[nodes[i]] += v.weight
		}
	}
	if len(st.byNode) == 0 {
		return fwk.NewStatus(fwk.Skip)
	}
	logger.V(4).Info("hermit volume locality", "pod", klog.KObj(pod), "byNode", st.byNode, "total", st.total)
	state.Write(stateKey, st)
	return nil
}

// Score is MaxNodeScore times the share of the pod's hermit capacity whose
// local copy is on this node.
func (p *Plugin) Score(_ context.Context, state fwk.CycleState, _ *v1.Pod, ni fwk.NodeInfo) (int64, *fwk.Status) {
	d, err := state.Read(stateKey)
	if err != nil {
		return 0, fwk.AsStatus(fmt.Errorf("%s: reading prescore state: %w", Name, err))
	}
	st := d.(*preScoreState)
	held := st.byNode[ni.Node().Name]
	if held == 0 || st.total == 0 {
		return 0, nil
	}
	// held <= total; scale in float to avoid overflow with large capacities.
	return int64(float64(held) / float64(st.total) * float64(fwk.MaxNodeScore)), nil
}

// ScoreExtensions: scores are already in [0, MaxNodeScore].
func (p *Plugin) ScoreExtensions() fwk.ScoreExtensions { return nil }
