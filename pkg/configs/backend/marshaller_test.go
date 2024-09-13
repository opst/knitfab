package backend_test

import (
	"testing"

	kback "github.com/opst/knitfab/pkg/configs/backend"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestConfigMarshall(t *testing.T) {
	t.Run("it loads config from yaml: ", func(t *testing.T) {
		backendYml := []byte(`
port: 12345
cluster:
  namespace: knit-testing-example
  database: db.knit-testing-example.svc.cluster.local
  dataAgent:
    image: knit-repo/dataagt:v0.0.1
    port: 8080
    volume:
      storageClassName: example-sc
      initialCapacity: 8Gi
  worker:
    priority: knit-worker-priority
    init:
      image: knit-repo/init:v0.0.2
    nurse:
      image: knit-repo/nurse:v0.0.3
      serviceAccount: fake-service-account
  keychains:
    signKeyForImportToken:
      name: fake-sign-key-name
`)
		result, err := kback.Unmarshal(backendYml)

		if err != nil {
			t.Errorf("failed to parse config.: %v", err)
		}

		t.Run(".port", func(t *testing.T) {
			actual := result.Port()
			expected := int32(12345)
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%d, %d)", expected, actual)
			}
		})

		t.Run(".cluster.namespace", func(t *testing.T) {
			actual := result.Cluster().Namespace()
			expected := "knit-testing-example"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%s, %s)", expected, actual)
			}
		})

		t.Run(".cluster.database", func(t *testing.T) {
			actual := result.Cluster().Database()
			expected := "db.knit-testing-example.svc.cluster.local"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%s, %s)", expected, actual)
			}
		})

		t.Run(".cluster.dataAgent.image", func(t *testing.T) {
			actual := result.Cluster().DataAgent().Image()
			expected := "knit-repo/dataagt:v0.0.1"
			if actual != expected {
				t.Errorf("mismatch. (actual, expected) = (%s, %s)", expected, actual)
			}
		})

		t.Run(".cluster.dataAgent.port", func(t *testing.T) {
			acutal := result.Cluster().DataAgent().Port()
			expected := int32(8080)
			if acutal != expected {
				t.Errorf("mismatch. (actual, expected) = (%d, %d)", expected, acutal)
			}
		})

		t.Run(".cluster.dataAgent.volume.storageClassName", func(t *testing.T) {
			actual := result.Cluster().DataAgent().Volume().StorageClassName()
			expected := "example-sc"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%s, %s)", expected, actual)
			}
		})

		t.Run(".cluster.dataAgent.volume.initialCapacity", func(t *testing.T) {
			actual := result.Cluster().DataAgent().Volume().InitialCapacity()
			expected := resource.MustParse("8Gi")
			if !expected.Equal(actual) {
				t.Errorf("mismatch. (expected, actual) = (%v, %v)", expected, actual)
			}
		})

		t.Run(".cluster.worker.init.image", func(t *testing.T) {
			actual := result.Cluster().Worker().Init().Image()
			expected := "knit-repo/init:v0.0.2"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%v, %v)", expected, actual)
			}
		})

		t.Run(".cluster.worker.priority", func(t *testing.T) {
			actual := result.Cluster().Worker().Priority()
			expected := "knit-worker-priority"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%v, %v)", expected, actual)
			}
		})

		t.Run(".cluster.worker.nurse.image", func(t *testing.T) {
			actual := result.Cluster().Worker().Nurse().Image()
			expected := "knit-repo/nurse:v0.0.3"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%v, %v)", expected, actual)
			}
		})

		t.Run(".cluster.worker.nurse.serviceAccount", func(t *testing.T) {
			actual := result.Cluster().Worker().Nurse().ServiceAccount()
			expected := "fake-service-account"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%v, %v)", expected, actual)
			}
		})

		t.Run(".cluster.keychain.names.signKeyForImportToken", func(t *testing.T) {
			actual := result.Cluster().Keychains().SignKeyForImportToken().Name()
			expected := "fake-sign-key-name"
			if actual != expected {
				t.Errorf("mismatch. (expected, actual) = (%v, %v)", expected, actual)
			}
		})
	})
}
