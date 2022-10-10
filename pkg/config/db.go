package config

import "github.com/go-sql-driver/mysql"

const (
	// DBNetSocket socket net type
	DBNetSocket = "unix"
	// DBNetTCP tcp net type
	DBNetTCP = "tcp"
)

// Parameter tuned parameters
type Parameter struct {
	InnodbBufPoolSize  uint `ini:"innodb_buffer_pool_size"`
	TableOpenCache     uint `ini:"table_open_cache"`
	TableDefCache      uint `ini:"table_definition_cache"`
	InnodbLogFileSize  uint `ini:"innodb_log_file_size"`
	InnodbLogBufSize   uint `ini:"innodb_log_buffer_size"`
	MaxConnections     uint `ini:"max_connections"`
	KeyBufSize         uint `ini:"key_buffer_size"`
	InnodbflushNBR     uint `ini:"innodb_flush_neighbors_"`
	InnodbBufPoolInst  uint `ini:"innodb_buffer_pool_instances"`
	TableOpenCacheInst uint `ini:"table_open_cache_instances"`
	InnodbIOCapMax     uint `ini:"innodb_io_capacity_max"`
	InnodbIOCap        uint `ini:"innodb_io_capacity"`
}

// ToDBConfig returns config for database
func (cfg *Config) ToDBConfig() *mysql.Config {
	dbCfg := &mysql.Config{
		User:                 cfg.Username,
		Passwd:               cfg.Password,
		AllowNativePasswords: true,
	}
	if len(cfg.Socket) > 0 {
		dbCfg.Net = DBNetSocket
		dbCfg.Addr = cfg.Socket
	} else {
		dbCfg.Net = DBNetTCP
		dbCfg.Addr = cfg.Hostname
	}

	return dbCfg
}
