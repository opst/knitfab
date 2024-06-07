package kubeutil

import (
	"log"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"k8s.io/client-go/rest"
)

// detect *kubernetes.Clientset.
//
// *CAUTION*: If no configs are found & the process is not running in cluster,
// IT WILL CAUSE PANIC.
//
// # It searches kubeconfig from
//
// - `~/.kube/config`
//
// - environmental variable `KUBECONFIG`
//
// - the file found first from the kubeConfigSearchPath
//
// When no files are found from above, it tries to use in-cluster config.
func ConnectToK8s(kubeconfigSearchPath ...string) *kubernetes.Clientset {

	kubeconfig := ""

	// priority 1 (least): ~/.kube/config
	if home := homedir.HomeDir(); home != "" {
		_kubeconfig := filepath.Join(home, ".kube", "config")
		if s, err := os.Stat(_kubeconfig); err == nil && !s.IsDir() {
			kubeconfig = _kubeconfig
		}
	}

	// priority 2: envvar KUBECONFIG
	if k := os.Getenv("KUBECONFIG"); k != "" {
		if s, err := os.Stat(k); err == nil && !s.IsDir() {
			kubeconfig = k
		}
	}

	// priority 3 (most): search path
	for _, sp := range kubeconfigSearchPath {
		if s, err := os.Stat(sp); err == nil && !s.IsDir() {
			kubeconfig = sp
			break
		}
	}

	var config *rest.Config
	var err error
	if kubeconfig == "" {
		// fallback: try in-cluster
		config, err = rest.InClusterConfig()
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	if err != nil {
		log.Fatalln(err) // PANIC!
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln(err) // PANIC!
	}
	return clientset
}
