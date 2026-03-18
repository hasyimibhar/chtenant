package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Proxy    ProxyConfig     `yaml:"proxy"`
	Admin    AdminConfig     `yaml:"admin"`
	Clusters []ClusterConfig `yaml:"clusters"`
	Postgres PostgresConfig  `yaml:"postgres"`
}

type ProxyConfig struct {
	HTTPAddr   string `yaml:"http_addr"`
	NativeAddr string `yaml:"native_addr"`
}

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

type ClusterConfig struct {
	ID             string `yaml:"id"`
	HTTPEndpoint   string `yaml:"http_endpoint"`
	NativeEndpoint string `yaml:"native_endpoint"`
	User           string `yaml:"user"`
	Password       string `yaml:"password"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Proxy: ProxyConfig{
			HTTPAddr:   ":8124",
			NativeAddr: ":9001",
		},
		Admin: AdminConfig{
			Addr: ":8125",
		},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
