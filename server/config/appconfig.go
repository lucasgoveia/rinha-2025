package config

import (
	"github.com/spf13/viper"
	"log"
)

type ServerConfig struct {
	Port   int    `mapstructure:"port"`
	Host   string `mapstructure:"host"`
	Socket string `mapstructure:"socket"`
}

type PostgresConfig struct {
	URL string `mapstructure:"url"`
}

type ServiceConfig struct {
	DefaultURL        string `mapstructure:"default_url"`
	DefaultHealthURL  string `mapstructure:"default_health_url"`
	FallbackURL       string `mapstructure:"fallback_url"`
	FallbackHealthURL string `mapstructure:"fallback_health_url"`
}

type AppConfig struct {
	Server   *ServerConfig   `mapstructure:"server"`
	Postgres *PostgresConfig `mapstructure:"postgres"`
	Service  *ServiceConfig  `mapstructure:"service"`
}

func LoadConfig() (*AppConfig, error) {

	viper.AutomaticEnv()

	viper.SetDefault("server.port", 1323)
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("service.default_url", "http://localhost:8001/payments")
	viper.SetDefault("service.fallback_url", "http://localhost:8002/payments")

	_ = viper.BindEnv("server.port", "SERVER_PORT")
	_ = viper.BindEnv("server.host", "SERVER_HOST")
	_ = viper.BindEnv("postgres.url", "POSTGRES_URL")
	_ = viper.BindEnv("service.default_url", "SERVICE_DEFAULT_URL")
	_ = viper.BindEnv("service.fallback_url", "SERVICE_FALLBACK_URL")
	_ = viper.BindEnv("service.default_health_url", "SERVICE_DEFAULT_HEALTH_URL")
	_ = viper.BindEnv("service.fallback_health_url", "SERVICE_FALLBACK_HEALTH_URL")
	_ = viper.BindEnv("server.socket", "SERVER_SOCKET")

	var config AppConfig
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	return &config, nil
}
