package locality

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/profile"

	"github.com/tsuzu/hermit-csi/pkg/manifest"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

const (
	ns            = "default"
	schedulerName = "hermit-scheduler"
	deployConfig  = "../../../deploy/scheduler/config.yaml"
)

func node(name string, unschedulable bool) *v1.Node {
	res := v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("4"), v1.ResourceMemory: resource.MustParse("8Gi"), v1.ResourcePods: resource.MustParse("110"),
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"kubernetes.io/hostname": name}},
		Spec:       v1.NodeSpec{Unschedulable: unschedulable},
		Status:     v1.NodeStatus{Capacity: res, Allocatable: res},
	}
}

// boundClaim returns a bound PVC/PV pair for a CSI volume of the given driver.
func boundClaim(name, driver, size string) (*v1.PersistentVolumeClaim, *v1.PersistentVolume) {
	q := resource.MustParse(size)
	pvName := "pv-" + name
	uid := types.UID("uid-pvc-" + name)
	// As the PV controller leaves a bound pair (VolumeBinding checks all of it).
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: uid, Annotations: map[string]string{
			"pv.kubernetes.io/bind-completed": "yes",
		}},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources:   v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: q}},
			VolumeName:  pvName,
		},
		Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: v1.PersistentVolumeSpec{
			Capacity:    v1.ResourceList{v1.ResourceStorage: q},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{
				Driver: driver, VolumeHandle: pvName,
			}},
			ClaimRef: &v1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: ns, Name: name, UID: uid},
		},
		Status: v1.PersistentVolumeStatus{Phase: v1.VolumeBound},
	}
	return pvc, pv
}

func pod(name string, claims ...string) *v1.Pod {
	p := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + name)},
		Spec: v1.PodSpec{
			SchedulerName: schedulerName,
			Containers:    []v1.Container{{Name: "c", Image: "busybox"}},
		},
	}
	for _, c := range claims {
		p.Spec.Volumes = append(p.Spec.Volumes, v1.Volume{Name: c, VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: c},
		}})
	}
	return p
}

func saveOwner(t *testing.T, s store.ObjectStore, vol string, o *manifest.Owner) {
	t.Helper()
	if _, err := manifest.SaveOwner(context.Background(), s, vol, o, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
}

// TestSchedulerPrefersLocalCopy runs the real kube-scheduler (against a fake
// API server) with the shipped deploy/scheduler/config.yaml and checks where
// pods are bound.
func TestSchedulerPrefersLocalCopy(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg, err := options.LoadConfigFromFile(klog.FromContext(ctx), deployConfig)
	if err != nil {
		t.Fatalf("load %s: %v", deployConfig, err)
	}

	s := store.NewMem()
	objs := []runtime.Object{
		node("node-1", false), node("node-2", false), node("node-3", false), node("node-4", false),
		node("node-cordoned", true),
	}
	addClaim := func(name, driver, size string) {
		pvc, pv := boundClaim(name, driver, size)
		objs = append(objs, pvc, pv)
	}
	// One volume last detached from each node: each pod must follow its volume
	// (passing by chance would be 1 in 4^4).
	for i := 1; i <= 4; i++ {
		addClaim(fmt.Sprintf("last-%d", i), DefaultDriverName, "10Gi")
		saveOwner(t, s, fmt.Sprintf("pv-last-%d", i), &manifest.Owner{Epoch: 2, LastNode: fmt.Sprintf("node-%d", i)})
	}
	// Two volumes on different nodes: the larger one (more to restore) wins.
	addClaim("big", DefaultDriverName, "100Gi")
	addClaim("small", DefaultDriverName, "1Gi")
	saveOwner(t, s, "pv-big", &manifest.Owner{NodeID: "node-2", Epoch: 1, LastNode: "node-4"}) // still attached
	saveOwner(t, s, "pv-small", &manifest.Owner{Epoch: 2, LastNode: "node-3"})
	// Holder cannot take pods: locality is only a preference.
	addClaim("on-cordoned", DefaultDriverName, "10Gi")
	saveOwner(t, s, "pv-on-cordoned", &manifest.Owner{Epoch: 2, LastNode: "node-cordoned"})
	// Volumes the plugin has no opinion on.
	addClaim("fresh", DefaultDriverName, "10Gi") // never staged: no owner record
	addClaim("other", "other.csi.example.com", "10Gi")

	client := fake.NewClientset(objs...)
	bound := make(chan [2]string, 16)
	client.PrependReactor("create", "pods", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "binding" {
			return false, nil, nil
		}
		b := a.(clienttesting.CreateAction).GetObject().(*v1.Binding)
		bound <- [2]string{b.Name, b.Target.Name}
		return true, b, nil
	})

	informerFactory := scheduler.NewInformerFactory(client, 0, nil)
	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})
	sched, err := scheduler.New(ctx, client, informerFactory, nil, profile.NewRecorderFactory(broadcaster),
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(frameworkruntime.Registry{Name: NewWithStore(s)}),
	)
	if err != nil {
		t.Fatal(err)
	}
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())
	go sched.Run(ctx)

	want := map[string]string{ // pod -> node ("*" = any schedulable node)
		"p-last-1": "node-1", "p-last-2": "node-2", "p-last-3": "node-3", "p-last-4": "node-4",
		"p-big-small":   "node-2",
		"p-on-cordoned": "*",
		"p-fresh":       "*",
		"p-other":       "*",
		"p-no-volumes":  "*",
	}
	pods := []*v1.Pod{
		pod("p-last-1", "last-1"), pod("p-last-2", "last-2"), pod("p-last-3", "last-3"), pod("p-last-4", "last-4"),
		pod("p-big-small", "big", "small"),
		pod("p-on-cordoned", "on-cordoned"),
		pod("p-fresh", "fresh"),
		pod("p-other", "other"),
		pod("p-no-volumes"),
	}
	for _, p := range pods {
		if _, err := client.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	got := map[string]string{}
	timeout := time.After(30 * time.Second)
	for len(got) < len(want) {
		select {
		case b := <-bound:
			got[b[0]] = b[1]
		case <-timeout:
			t.Fatalf("timed out; bound so far: %v", got)
		}
	}
	for p, w := range want {
		switch g := got[p]; {
		case g == "node-cordoned":
			t.Errorf("%s bound to the cordoned node", p)
		case w != "*" && g != w:
			t.Errorf("%s bound to %s, want %s", p, g, w)
		}
	}
	t.Logf("bindings: %v", got)
}

// failingStore fails every read, like an unreachable S3.
type failingStore struct{ store.ObjectStore }

func (failingStore) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", errors.New("connection refused")
}

func newTestPlugin(t *testing.T, s store.ObjectStore, objs ...runtime.Object) *Plugin {
	t.Helper()
	f := informers.NewSharedInformerFactory(fake.NewClientset(objs...), 0)
	p := &Plugin{
		driver: DefaultDriverName, timeout: time.Second, s: s,
		pvcs: f.Core().V1().PersistentVolumeClaims().Lister(),
		pvs:  f.Core().V1().PersistentVolumes().Lister(),
	}
	f.Start(t.Context().Done())
	f.WaitForCacheSync(t.Context().Done())
	return p
}

func TestPreScoreAndScore(t *testing.T) {
	pvcA, pvA := boundClaim("a", DefaultDriverName, "3Gi")
	pvcB, pvB := boundClaim("b", DefaultDriverName, "1Gi")
	pvcO, pvO := boundClaim("o", "other.csi.example.com", "1Gi")
	unbound := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "unbound"}}
	s := store.NewMem()
	saveOwner(t, s, "pv-a", &manifest.Owner{Epoch: 2, LastNode: "n1"})
	saveOwner(t, s, "pv-b", &manifest.Owner{NodeID: "n2", Epoch: 1})
	p := newTestPlugin(t, s, pvcA, pvA, pvcB, pvB, pvcO, pvO, unbound)

	scores := func(pl *Plugin, po *v1.Pod) (map[string]int64, *fwk.Status) {
		state := framework.NewCycleState()
		if st := pl.PreScore(t.Context(), state, po, nil); !st.IsSuccess() {
			return nil, st
		}
		out := map[string]int64{}
		for _, n := range []string{"n1", "n2", "n3"} {
			ni := framework.NewNodeInfo()
			ni.SetNode(node(n, false))
			sc, st := pl.Score(t.Context(), state, po, ni)
			if !st.IsSuccess() {
				t.Fatalf("Score %s: %v", n, st)
			}
			out[n] = sc
		}
		return out, nil
	}

	// Weighted by capacity: 3Gi on n1, 1Gi on n2; the other driver, the
	// unbound claim and a missing claim are ignored.
	got, st := scores(p, pod("x", "a", "b", "o", "unbound", "missing"))
	if st != nil {
		t.Fatalf("PreScore: %v", st)
	}
	if got["n1"] != 75 || got["n2"] != 25 || got["n3"] != 0 {
		t.Fatalf("scores = %v, want n1=75 n2=25 n3=0", got)
	}

	// Nothing to prefer: Skip, so the framework does not call Score.
	for name, po := range map[string]*v1.Pod{
		"no volumes":       pod("y"),
		"other driver":     pod("z", "o"),
		"unbound":          pod("w", "unbound"),
		"no owner records": pod("v", "a"),
	} {
		pl := p
		if name == "no owner records" {
			pl = newTestPlugin(t, store.NewMem(), pvcA, pvA)
		}
		if _, st := scores(pl, po); st.Code() != fwk.Skip {
			t.Errorf("%s: PreScore = %v, want Skip", name, st)
		}
	}

	// An unreachable S3 never fails scheduling: the volume is just ignored.
	if _, st := scores(newTestPlugin(t, failingStore{store.NewMem()}, pvcA, pvA), pod("u", "a")); st.Code() != fwk.Skip {
		t.Fatalf("S3 down: PreScore = %v, want Skip", st)
	}
}

func TestArgs(t *testing.T) {
	a, timeout, err := parseArgs(&runtime.Unknown{Raw: []byte(`{"bucket":"b","timeout":"500ms"}`), ContentType: runtime.ContentTypeJSON})
	if err != nil || a.Bucket != "b" || a.DriverName != DefaultDriverName || timeout != 500*time.Millisecond {
		t.Fatalf("parseArgs = %+v %v %v", a, timeout, err)
	}
	if _, _, err := parseArgs(&runtime.Unknown{Raw: []byte(`{"timeout":"soon"}`), ContentType: runtime.ContentTypeJSON}); err == nil {
		t.Fatal("invalid timeout accepted")
	}

	env := map[string]string{
		"S3_BUCKET": "env-bucket", "S3_PREFIX": "cluster-1", "S3_ENDPOINT": "http://minio:9000", "S3_PATH_STYLE": "true",
	}
	c := storeConfig(Args{Prefix: "from-args"}, func(k string) string { return env[k] })
	want := store.Config{Bucket: "env-bucket", Prefix: "from-args", Endpoint: "http://minio:9000", Region: "us-east-1", PathStyle: true}
	if c != want {
		t.Fatalf("storeConfig = %+v, want %+v", c, want)
	}
}

// fakeHandle provides only what the plugin factory uses.
type fakeHandle struct {
	fwk.Handle
	f informers.SharedInformerFactory
}

func (h fakeHandle) SharedInformerFactory() informers.SharedInformerFactory { return h.f }

func TestNewFromEnvironment(t *testing.T) {
	h := fakeHandle{f: informers.NewSharedInformerFactory(fake.NewClientset(), 0)}
	args := &runtime.Unknown{Raw: []byte(`{"timeout":"1s"}`), ContentType: runtime.ContentTypeJSON}

	t.Setenv("S3_BUCKET", "")
	if _, err := New(t.Context(), args, h); err == nil {
		t.Fatal("New without a bucket should fail")
	}
	t.Setenv("S3_BUCKET", "hermit")
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:9")
	pl, err := New(t.Context(), args, h)
	if err != nil {
		t.Fatal(err)
	}
	if p := pl.(*Plugin); p.Name() != Name || p.timeout != time.Second || p.driver != DefaultDriverName {
		t.Fatalf("plugin = %+v", p)
	}
}
