//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	"context"
	_ "embed"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	connk8s "github.com/opst/knitfab/pkg/conn/k8s"
	"github.com/opst/knitfab/pkg/utils"
	kubecore "k8s.io/api/core/v1"
	kubeapierr "k8s.io/apimachinery/pkg/api/errors"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//go:embed CREDITS
var CREDITS string

// track k8s container log and dump them into a file.
//
// This command can be run in *nix system only. (depends on SIGTERM for aborting)
//
// USAGE
//
//	log_recorder <CONTAINER_NAME> <DEST>
//
// PARAMETERS
//
//	CONTAINTER_NAME : The container name which is source of log.
//
//	DEST : Filepath where log to be written.
//	      Parent directories are not created automaticaly.
//	      If there is a file/directory at the filepath, it will cause an error.
//
// ENVIRONMENT VARIABLE
//
//	POD_NAME : (required) The pod name which has the container CONTAINER_NAME.
//
//	NAMESPACE : (required) The namespace where the pod POD_NAME is placed in.
//
// DESCRIPTION
//
//	This command is designed to be used as a side-car container of k8s pod.
//
//	Required envvars, POD_NAME and NAMESPACE, can be decided like
//
//	    serviceAccountName: log-recording-service-account
//	    #                   with role { apiGroups: [""], resources: ["pods", "pods/log"], verbs: ["get"] }
//	    containers:
//	        - name: main
//	          # ... other container specs ...
//
//	        - name: nurse
//	          image: ...
//	          cmd: ["log_recorder"]
//	          args: ["main", "/dest/log"]
//	          volumeMounts:
//	              - name: log-store
//	                mountPath: "/dest"
//	          env:
//	            - name: POD_NAME
//	              valueFrom:
//	                  fieldRef:
//	                      fieldPath: "metadata.name"
//	            - name: NAMESPACE
//	              valueFrom:
//	                  fieldRef:
//	                      fieldPath: "metadata.namespace"
//
//	This is the reason why POD_NAME and NAMESPACE is passed as envvars,
//	rather than command-line parameters.
func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--lisence" {
			log.Println(CREDITS)
			return
		}
	}

	container := os.Args[1]
	dest := os.Args[2]

	f, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR|os.O_EXCL, os.FileMode(0777))
	if err != nil {
		log.Fatalln("failed to open logfile: ", err)
	}
	defer func() { f.Sync(); f.Close() }()

	pod := os.Getenv("POD_NAME")
	if pod == "" {
		log.Fatalln("required envvar: POD_NAME - set the name of pod which is a log source")
	}

	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		log.Fatalln("required envvar: NAMESPACE - set the namespace where the pod is placed in")
	}

	// listen signals to terminate
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt, syscall.SIGTERM, os.Kill,
	)
	defer cancel()

	k8sclient := connk8s.ConnectToK8s()

	// wait for target container getting up...
	lastWaitingMessage := time.Now()
	for {
		p, err := k8sclient.CoreV1().
			Pods(namespace).
			Get(ctx, pod, kubeapimeta.GetOptions{})

		if err != nil && !kubeapierr.IsBadRequest(err) {
			log.Fatalf("cannot get pod: \"%s\": %s", pod, err)
		}

		cs, ok := utils.First(p.Status.ContainerStatuses, func(cs kubecore.ContainerStatus) bool {
			return cs.Name == container
		})

		if !ok {
			log.Fatalf(
				"container missing?: \"%s\" (in namespace: \"%s\", pod: \"%s\")\n",
				container, namespace, pod,
			)
		}

		if cs.State.Waiting == nil {
			break
		}

		now := time.Now()
		if 1 < now.Sub(lastWaitingMessage).Seconds() {
			log.Printf("waiting container %s in pod %s ...", container, pod)
			lastWaitingMessage = now
		}
	}

	// pull log and write it to file...
	l, err := k8sclient.CoreV1().
		Pods(namespace).
		GetLogs(pod, &kubecore.PodLogOptions{
			Container: container,
			Follow:    true,
		}).
		Stream(ctx)

	if err != nil {
		log.Fatalf(
			"cannot get logs of container: \"%s\" (in namespace: \"%s\", pod: \"%s\") : %s\n",
			container, namespace, pod, err,
		)
	}

	defer l.Close()

	if _, err := io.Copy(f, l); err != nil {
		log.Fatalln("error in log tracing: ", err)
	}
}
