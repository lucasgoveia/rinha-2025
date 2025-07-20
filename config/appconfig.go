package config

import (
	"github.com/spf13/viper"
	"log"
)

type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Host string `mapstructure:"host"`
}

type PostgresConfig struct {
	URL string `mapstructure:"url"`
}

type RedisConfig struct {
	URL          string `mapstructure:"url"`
	StreamName   string `mapstructure:"stream_name"`
	StreamGroup  string `mapstructure:"stream_group"`
	ConsumerName string `mapstructure:"consumer_name"`
}

type TelemetryConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	ServiceName string `mapstructure:"service_name"`
	JaegerURL   string `mapstructure:"jaeger_url"`
}

type ServiceConfig struct {
	DefaultURL  string `mapstructure:"default_url"`
	FallbackURL string `mapstructure:"fallback_url"`
}

type AppConfig struct {
	Server    *ServerConfig    `mapstructure:"server"`
	Postgres  *PostgresConfig  `mapstructure:"postgres"`
	Redis     *RedisConfig     `mapstructure:"redis"`
	Telemetry *TelemetryConfig `mapstructure:"telemetry"`
	Service   *ServiceConfig   `mapstructure:"service"`
}

func LoadConfig() (*AppConfig, error) {

	viper.AutomaticEnv()

	viper.SetDefault("server.port", 1323)
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("telemetry.enabled", false)
	viper.SetDefault("telemetry.service_name", "api-service")
	viper.SetDefault("telemetry.jaeger_url", "http://jaeger:14268/api/traces")
	viper.SetDefault("service.default_url", "http://localhost:8001/payments")
	viper.SetDefault("service.fallback_url", "http://localhost:8002/payments")

	_ = viper.BindEnv("server.port", "SERVER_PORT")
	_ = viper.BindEnv("server.host", "SERVER_HOST")
	_ = viper.BindEnv("postgres.url", "POSTGRES_URL")
	_ = viper.BindEnv("redis.url", "REDIS_URL")
	_ = viper.BindEnv("telemetry.enabled", "TELEMETRY_ENABLED")
	_ = viper.BindEnv("telemetry.service_name", "TELEMETRY_SERVICE_NAME")
	_ = viper.BindEnv("telemetry.jaeger_url", "JAEGER_URL")
	_ = viper.BindEnv("service.default_url", "SERVICE_DEFAULT_URL")
	_ = viper.BindEnv("service.fallback_url", "SERVICE_FALLBACK_URL")
	_ = viper.BindEnv("redis.stream_name", "REDIS_STREAM_NAME")
	_ = viper.BindEnv("redis.stream_group", "REDIS_STREAM_GROUP")
	_ = viper.BindEnv("redis.consumer_name", "REDIS_CONSUMER_NAME")

	//viper.SetConfigFile("config/config.yaml")
	//if err := viper.ReadInConfig(); err != nil {
	//	log.Fatalf("Error reading config file, %s", err)
	//}

	var config AppConfig
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	return &config, nil
}
