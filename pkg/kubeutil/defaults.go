package kubeutil

import (
	"flag"
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
// It searches kubeconfig from
//
// - `~/.kube/config`
//
// - environmental variable `KUBECONFIG`
//
// - command line flag `-kubeconfig`
//
// When no files are found from above, it tries to use in-cluster config.
func ConnectToK8s() *kubernetes.Clientset {

	kubeconfig := ""

	// priority 1 (least): ~/.kube/config
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	// priority 2: envvar KUBECONFIG
	if k := os.Getenv("KUBECONFIG"); k != "" {
		kubeconfig = k
	}

	// priority 3 (most): flag -kubeconfig
	{
		k := flag.String("kubeconfig", kubeconfig, "(optional) path to kubeconfig file")
		flag.Parse()

		if k != nil && *k != "" {
			kubeconfig = *k
		}
	}

	if kubeconfig != "" {
		stat, err := os.Stat(kubeconfig)
		if os.IsNotExist(err) || stat.IsDir() {
			kubeconfig = ""
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
