package pvcs_test

import (
	"context"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	"github.com/opst/knitfab/pkg/workloads/k8s/testenv"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pvcs "github.com/opst/knitfab/cmd/volume_expander/pvcs"
)

func TestObserveOnNode(t *testing.T) {
	ctx := context.Background()
	if dl, ok := t.Deadline(); ok {
		_ctx, cancel := context.WithDeadline(ctx, dl.Add(-100*time.Millisecond))
		defer cancel()
		ctx = _ctx
	}

	cs := testenv.NewClient()
	pvcNameA := "pvc-test-observer-a"
	pvcNameB := "pvc-test-observer-b"

	{
		defer func() {
			cs.CoreV1().PersistentVolumeClaims(testenv.Namespace()).Delete(
				context.Background(),
				pvcNameA,
				metav1.DeleteOptions{},
			)
		}()
		try.To(cs.CoreV1().PersistentVolumeClaims(testenv.Namespace()).Create(
			ctx,
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcNameA},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			},
			metav1.CreateOptions{},
		)).OrFatal(t)
	}
	{
		defer func() {
			cs.CoreV1().PersistentVolumeClaims(testenv.Namespace()).Delete(
				context.Background(),
				pvcNameB,
				metav1.DeleteOptions{},
			)
		}()
		try.To(cs.CoreV1().PersistentVolumeClaims(testenv.Namespace()).Create(
			ctx,
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcNameB},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			},
			metav1.CreateOptions{},
		)).OrFatal(t)
	}

	var nodeName string
	{
		nodes := try.To(cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})).OrFatal(t)
		node := nodes.Items[0]
		nodeName = node.Name
	}

	testee := try.To(pvcs.ObserveOnNode(ctx, cs, nodeName, testenv.Namespace())).OrFatal(t)
	defer testee.Close()

	if testee.Closed() {
		t.Fatal("observer is closed")
	}

	if _, ok := testee.StatusOf(pvcNameA); ok {
		t.Errorf("[no pods] expected %s not to be observed", pvcNameA)
	}
	if _, ok := testee.StatusOf(pvcNameB); ok {
		t.Errorf("[no pods] expected %s not to be observed", pvcNameB)
	}

	reader := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-observer-reader"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "c", Image: "busybox:1.36.1",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "a", MountPath: "/mnt/1"},
						{Name: "b", MountPath: "/mnt/2", ReadOnly: true},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "a",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcNameA, ReadOnly: true,
						},
					},
				},
				{
					Name: "b",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcNameB, ReadOnly: false,
						},
					},
				},
			},
			Affinity: &corev1.Affinity{
				// force to deploy on the target node
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchFields: []corev1.NodeSelectorRequirement{
									{
										Key:      "metadata.name",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{nodeName},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	defer func() {
		cs.CoreV1().Pods(testenv.Namespace()).Delete(
			context.Background(), reader.Name, metav1.DeleteOptions{},
		)
	}()
	try.To(
		cs.CoreV1().Pods(testenv.Namespace()).Create(
			ctx, &reader, metav1.CreateOptions{},
		),
	).OrFatal(t)

	for {
		pod := try.To(cs.CoreV1().Pods(testenv.Namespace()).Get(
			ctx, reader.ObjectMeta.Name, metav1.GetOptions{},
		)).OrFatal(t)
		if pod.Status.Phase == corev1.PodRunning {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if stat, ok := testee.StatusOf(pvcNameA); !ok {
		t.Errorf("[RO pod is started] expected %s to be observed", pvcNameA)
	} else {
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; !ok {
			t.Errorf("[RO pod is started] expected %s to be mounted on %s", pvcNameA, reader.ObjectMeta.Name)
		}
		if !stat.ReadOnly() {
			t.Errorf("[RO pod is started] expected %s to be read-only", pvcNameA)
		}
	}
	if stat, ok := testee.StatusOf(pvcNameB); !ok {
		t.Errorf("[RO pod is started] expected %s to be observed", pvcNameB)
	} else {
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; !ok {
			t.Errorf("[RO pod is started] expected %s to be mounted on %s", pvcNameB, reader.ObjectMeta.Name)
		}
		if !stat.ReadOnly() {
			t.Errorf("[RO pod is started] expected %s to be read-only", pvcNameB)
		}
	}

	writer := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-observer-writer"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "c", Image: "busybox:1.36.1",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "a", MountPath: "/mnt/a"},
						{Name: "b", MountPath: "/mnt/b"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "a",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcNameA, ReadOnly: false,
						},
					},
				},
				{
					Name: "b",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcNameB, ReadOnly: false,
						},
					},
				},
			},
			Affinity: &corev1.Affinity{
				// force to deploy on the target node
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchFields: []corev1.NodeSelectorRequirement{
									{
										Key:      "metadata.name",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{nodeName},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	defer func() {
		cs.CoreV1().Pods(testenv.Namespace()).Delete(
			context.Background(), writer.Name, metav1.DeleteOptions{},
		)
	}()
	try.To(
		cs.CoreV1().Pods(testenv.Namespace()).Create(
			ctx,
			&writer,
			metav1.CreateOptions{},
		),
	).OrFatal(t)
	for {
		pod := try.To(cs.CoreV1().Pods(testenv.Namespace()).Get(
			ctx,
			writer.Name,
			metav1.GetOptions{},
		)).OrFatal(t)
		if pod.Status.Phase == corev1.PodRunning {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if stat, ok := testee.StatusOf(pvcNameA); !ok {
		t.Errorf("[RW pod is started] expected %s to be observed", pvcNameA)
	} else {
		if _, ok := stat.MountedOn[writer.ObjectMeta.Name]; !ok {
			t.Errorf("[RW pod is started] expected %s to be mounted on %s", pvcNameA, writer.ObjectMeta.Name)
		}
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; !ok {
			t.Errorf("[RW pod is started] expected %s to be mounted on %s", pvcNameA, reader.ObjectMeta.Name)
		}
		if stat.ReadOnly() {
			t.Errorf("[RW pod is started] expected %s to be read-write", pvcNameA)
		}
	}
	if stat, ok := testee.StatusOf(pvcNameB); !ok {
		t.Errorf("[RW pod is started] expected %s to be observed", pvcNameB)
	} else {
		if _, ok := stat.MountedOn[writer.ObjectMeta.Name]; !ok {
			t.Errorf("[RW pod is started] expected %s to be mounted on %s", pvcNameB, writer.ObjectMeta.Name)
		}
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; !ok {
			t.Errorf("[RW pod is started] expected %s to be mounted on %s", pvcNameB, reader.ObjectMeta.Name)
		}
		if stat.ReadOnly() {
			t.Errorf("[RW pod is started] expected %s to be read-write", pvcNameB)
		}
	}

	if err := cs.CoreV1().Pods(testenv.Namespace()).Delete(
		context.Background(), writer.Name, metav1.DeleteOptions{
			GracePeriodSeconds: pointer.Ref[int64](0),
		},
	); err != nil {
		t.Fatal(err)
	}
	for {
		_, err := cs.CoreV1().Pods(testenv.Namespace()).Get(
			ctx,
			writer.Name,
			metav1.GetOptions{},
		)
		if k8serrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if stat, ok := testee.StatusOf(pvcNameA); !ok {
		t.Errorf("[RW pod is deleted] expected %s to be observed", pvcNameA)
	} else {
		if _, ok := stat.MountedOn[writer.ObjectMeta.Name]; ok {
			t.Errorf("[RW pod is deleted] expected %s not to be mounted on %s", pvcNameA, writer.ObjectMeta.Name)
		}
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; !ok {
			t.Errorf("[RW pod is deleted] expected %s to be mounted on %s", pvcNameA, reader.ObjectMeta.Name)
		}
		if !stat.ReadOnly() {
			t.Errorf("[RW pod is deleted] expected %s to be read-only", pvcNameA)
		}
	}
	if stat, ok := testee.StatusOf(pvcNameB); !ok {
		t.Errorf("[RW pod is deleted] expected %s to be observed", pvcNameB)
	} else {
		if _, ok := stat.MountedOn[writer.ObjectMeta.Name]; ok {
			t.Errorf("[RW pod is deleted] expected %s not to be mounted on %s", pvcNameB, writer.ObjectMeta.Name)
		}
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; !ok {
			t.Errorf("[RW pod is deleted] expected %s to be mounted on %s", pvcNameB, reader.ObjectMeta.Name)
		}
		if !stat.ReadOnly() {
			t.Errorf("[RW pod is deleted] expected %s to be read-only", pvcNameB)
		}
	}

	if err := cs.CoreV1().Pods(testenv.Namespace()).Delete(
		context.Background(), reader.Name, metav1.DeleteOptions{
			GracePeriodSeconds: pointer.Ref[int64](0),
		},
	); err != nil {
		t.Fatal(err)
	}
	for {
		_, err := cs.CoreV1().Pods(testenv.Namespace()).Get(
			ctx,
			reader.Name,
			metav1.GetOptions{},
		)
		if k8serrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if stat, ok := testee.StatusOf(pvcNameA); ok {
		t.Errorf("[RO pod is deleted] expected %s not to be observed", pvcNameA)
	} else {
		if _, ok := stat.MountedOn[writer.ObjectMeta.Name]; ok {
			t.Errorf("[RO pod is deleted] expected %s not to be mounted on %s", pvcNameA, writer.ObjectMeta.Name)
		}
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; ok {
			t.Errorf("[RO pod is deleted] expected %s not to be mounted on %s", pvcNameA, reader.ObjectMeta.Name)
		}
		if !stat.ReadOnly() {
			t.Errorf("[RO pod is deleted] expected %s to be read-only", pvcNameA)
		}
	}
	if stat, ok := testee.StatusOf(pvcNameB); ok {
		t.Errorf("[RO pod is deleted] expected %s not to be observed", pvcNameB)
	} else {
		if _, ok := stat.MountedOn[writer.ObjectMeta.Name]; ok {
			t.Errorf("[RO pod is deleted] expected %s not to be mounted on %s", pvcNameB, writer.ObjectMeta.Name)
		}
		if _, ok := stat.MountedOn[reader.ObjectMeta.Name]; ok {
			t.Errorf("[RO pod is deleted] expected %s not to be mounted on %s", pvcNameB, reader.ObjectMeta.Name)
		}
		if !stat.ReadOnly() {
			t.Errorf("[RO pod is deleted] expected %s to be read-only", pvcNameB)
		}
	}

	testee.Close()

	for !testee.Closed() {
		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
}
