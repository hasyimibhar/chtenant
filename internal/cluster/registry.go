package cluster

import "fmt"

type Cluster struct {
	ID             string
	HTTPEndpoint   string
	NativeEndpoint string
	User           string
	Password       string
}

type Registry interface {
	Get(id string) (*Cluster, error)
}

type StaticRegistry struct {
	clusters map[string]*Cluster
}

func NewStaticRegistry(clusters []Cluster) *StaticRegistry {
	m := make(map[string]*Cluster, len(clusters))
	for i := range clusters {
		m[clusters[i].ID] = &clusters[i]
	}
	return &StaticRegistry{clusters: m}
}

func (r *StaticRegistry) Get(id string) (*Cluster, error) {
	c, ok := r.clusters[id]
	if !ok {
		return nil, fmt.Errorf("cluster %q not found", id)
	}
	return c, nil
}
