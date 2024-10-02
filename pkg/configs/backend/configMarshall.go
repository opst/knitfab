package backend

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

type Marshalled[S any] interface {
	trySeal(string) S
}

// seal marshalled object.
//
// this function CAN CAUSE PANIC if misconfiguration is found.
//
// All types named `pkg/configs/backend.XxxMarshall` are `Marshalled[*Xxx]` .
func TrySeal[S any](conf Marshalled[S]) S {
	return conf.trySeal("(root)")
}

type BackendConfigMarshall struct {
	Port    int32                      `yaml:"port"`
	Cluster *KnitClusterConfigMarshall `yaml:"cluster"`
}

var _ Marshalled[*BackendConfig] = &BackendConfigMarshall{}

func (b *BackendConfigMarshall) trySeal(path string) *BackendConfig {
	return &BackendConfig{
		port:    b.Port,
		cluster: b.Cluster.trySeal(path + ".cluster"),
	}
}

// Configuration of knit cluster.
//
// This type is marshalling value and mutable.
// Consider to use immutable version, `KnitClusterConfig`.
// You can get `KnitClusterConfig` instance with `KnitCluserConfigMarshall.TrySeal()`
type KnitClusterConfigMarshall struct {
	Namespace string                   `yaml:"namespace"`
	Domain    string                   `yaml:"domain,omitempty"`
	Database  string                   `yaml:"database"`
	DataAgent *DataAgentConfigMarshall `yaml:"dataAgent"`
	Worker    *WorkerConfigMarshall    `yaml:"worker"`
	Keychains *KeychainsConfigMarshall `yaml:"keychains"`
}

// verify configuration value and create "readonly" version of this.
//
// IT WILL PANIC if any misconfiguration is found.
func (km *KnitClusterConfigMarshall) TrySeal() *KnitClusterConfig {
	return km.trySeal("(root)")
}

func (km *KnitClusterConfigMarshall) trySeal(path string) *KnitClusterConfig {
	domain := km.Domain
	if domain == "" {
		domain = "cluster.local"
	}
	return &KnitClusterConfig{
		namespace: required(km.Namespace, path+".namespace"),
		domain:    required(domain, path+".domain"),
		database:  required(km.Database, path+".database"),
		dataAgent: nonnil(km.DataAgent, path+".dataAgent").trySeal(path + ".dataAgent"),
		worker:    nonnil(km.Worker, path+".worker").trySeal(path + ".worker"),
		keychains: nonnil(km.Keychains, path+".keychain").trySeal(path + ".keychain"),
	}
}

type DataAgentConfigMarshall struct {
	Image  string                `yaml:"image"`
	Volume *VolumeConfigMarshall `yaml:"volume"`
	Port   int32                 `yaml:"port"`
}

func (wm *DataAgentConfigMarshall) trySeal(path string) *DataAgentConfig {
	return &DataAgentConfig{
		image:  required(wm.Image, path+".image"),
		volume: nonnil(wm.Volume, path+".volume").trySeal(path + ".volume"),
		port:   required(wm.Port, path+".port"),
	}
}

type VolumeConfigMarshall struct {
	StorageClassName string `yaml:"storageClassName"`
	InitialCapacity  string `yaml:"initialCapacity"`
}

func (dm *VolumeConfigMarshall) trySeal(path string) *VolumeConfig {
	ic, err := resource.ParseQuantity(
		required(dm.InitialCapacity, path+".initialCapacity"),
	)
	if err != nil {
		panic(fmt.Errorf("%s.initialCapacity can not be parsed: %w", path, err))
	}

	return &VolumeConfig{
		storageClassName: required(dm.StorageClassName, path+".storageClassName"),
		initialCapacity:  ic,
	}
}

type WorkerConfigMarshall struct {
	Priority string                        `yaml:"priority"`
	Init     *InitContainerConfigMarshall  `yaml:"init"`
	Nurse    *NurseContainerConfigMarshall `yaml:"nurse"`
}

func (wc *WorkerConfigMarshall) trySeal(path string) *WorkerConfig {
	return &WorkerConfig{
		priority: required(wc.Priority, path+".priority"),
		init:     nonnil(wc.Init, path+".init").trySeal(path + ".init"),
		nurse:    nonnil(wc.Nurse, path+".nurse").trySeal(path + ".nurse"),
	}
}

type InitContainerConfigMarshall struct {
	Image string `yaml:"image"`
}

func (i *InitContainerConfigMarshall) trySeal(path string) *InitContainerConfig {
	return &InitContainerConfig{
		image: required(i.Image, path+".image"),
	}
}

type NurseContainerConfigMarshall struct {
	Image                string `yaml:"image"`
	ServiceAccountSecret string `yaml:"serviceAccountSecret"`
}

func (n *NurseContainerConfigMarshall) trySeal(path string) *NurseContainerConfig {
	return &NurseContainerConfig{
		image:               required(n.Image, path+".image"),
		serviceAccountSeret: required(n.ServiceAccountSecret, path+".serviceAccountSecret"),
	}
}

type KeychainsConfigMarshall struct {
	SignKeyForImportToken *HS256KeyChainMarshall `yaml:"signKeyForImportToken"`
}

func (kc *KeychainsConfigMarshall) trySeal(path string) *KeychainsConfig {
	return &KeychainsConfig{
		signKeyForImportToken: nonnil(kc.SignKeyForImportToken, path+".signKeyForImportToken").trySeal(path + ".signKeyForImportToken"),
	}
}

type HS256KeyChainMarshall struct {
	Name string `yaml:"name"`
}

func (kn *HS256KeyChainMarshall) trySeal(path string) *HS256KeychainsConfig {
	return &HS256KeychainsConfig{
		name: required(kn.Name, path+".name"),
	}
}

func nonnil[T any](v *T, path string) *T {
	if v == nil {
		panic(path + " is required")
	}
	return v
}

func required[T comparable](v T, path string) T {
	if v == *new(T) {
		panic(path + " is required")
	}
	return v
}
