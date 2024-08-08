package backend

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

type BackendConfig struct {
	port    int32
	cluster *KnitClusterConfig
}

func (c *BackendConfig) Port() int32 {
	return c.port
}

func (c *BackendConfig) Cluster() *KnitClusterConfig {
	return c.cluster
}

// Configuration for Knit cluster.
//
// to get `KnitClusterConfig` instance, use `KnitClusterConfigMarshall.TrySeal()` .
type KnitClusterConfig struct {
	namespace string
	domain    string
	database  string
	dataAgent *DataAgentConfig
	worker    *WorkerConfig
	keychains *KeychainsConfig
}

// k8s namespace where Knit is deploied.
func (k *KnitClusterConfig) Namespace() string {
	return k.namespace
}

// k8s domain where Knit is deploied. default = "cluster.local"
func (k *KnitClusterConfig) Domain() string {
	return k.domain
}

// Connection string for database.
func (k *KnitClusterConfig) Database() string {
	return k.database
}

// Configuration for Dataagt
func (k *KnitClusterConfig) DataAgent() *DataAgentConfig {
	return k.dataAgent
}

// Configration for Worker
func (k *KnitClusterConfig) Worker() *WorkerConfig {
	return k.worker
}

func (l *KnitClusterConfig) Keychains() *KeychainsConfig {
	return l.keychains
}

// Configuration for Dataagt
type DataAgentConfig struct {
	image  string
	volume *VolumeConfig
	port   int32
}

// Which image should be used as Dataagt
func (c *DataAgentConfig) Image() string {
	return c.image
}

// Volume configuration of Datagt
func (c *DataAgentConfig) Volume() *VolumeConfig {
	return c.volume
}

func (c *DataAgentConfig) Port() int32 {
	return c.port
}

// Setting for volumes.
type VolumeConfig struct {
	storageClassName string
	initialCapacity  resource.Quantity
}

// What storage class should be used.
func (d *VolumeConfig) StorageClassName() string {
	return d.storageClassName
}

// How large should be PV in initial.
func (d *VolumeConfig) InitialCapacity() resource.Quantity {
	return d.initialCapacity
}

type WorkerConfig struct {
	priority string
	init     *InitContainerConfig
	nurse    *NurseContainerConfig
}

func (wc *WorkerConfig) Priority() string {
	return wc.priority
}

func (wc *WorkerConfig) Init() *InitContainerConfig {
	return wc.init
}

func (wc *WorkerConfig) Nurse() *NurseContainerConfig {
	return wc.nurse
}

type InitContainerConfig struct {
	image string
}

func (icc *InitContainerConfig) Image() string {
	return icc.image
}

type NurseContainerConfig struct {
	image          string
	serviceAccount string
}

func (ncc *NurseContainerConfig) Image() string {
	return ncc.image
}

func (ncc *NurseContainerConfig) ServiceAccount() string {
	return ncc.serviceAccount
}

type KeychainsConfig struct {
	signKeyForImportToken *HS256KeychainsConfig
}

func (kc *KeychainsConfig) SignKeyForImportToken() *HS256KeychainsConfig {
	return kc.signKeyForImportToken
}

type HS256KeychainsConfig struct {
	name string
}

func (kc *HS256KeychainsConfig) Name() string {
	return kc.name
}
