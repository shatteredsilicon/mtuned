package db

import "fmt"

// GlobalVariables structure of global variables
type GlobalVariables struct {
	InnodbBufferPoolSize    uint64 `db:"@@innodb_buffer_pool_size"`
	InnodbLogBufferSize     uint64 `db:"@@innodb_log_buffer_size"`
	KeyBufSize              uint64 `db:"@@key_buffer_size"`
	QueryCacheSize          uint64 `db:"query_cache_size"`
	MaxConnections          uint64 `db:"@@max_connections"`
	JoinBufferSize          uint64 `db:"@@join_buffer_size"`
	ReadBufferSize          uint64 `db:"@@read_buffer_size"`
	ReadRNDBufferSize       uint64 `db:"@@read_rnd_buffer_size"`
	SortBufferSize          uint64 `db:"@@sort_buffer_size"`
	TmpTableSize            uint64 `db:"@@tmp_table_size"`
	TableOpenCache          uint64 `db:"@@table_open_cache"`
	TableDefinitionCache    uint64 `db:"@@table_definition_cache"`
	InnodbLogFileSize       uint64 `db:"@@innodb_log_file_size"`
	InnodbLogFilesInGroup   uint64 `db:"@@innodb_log_files_in_group"`
	InnodbFlushNeighbors    uint8  `db:"@@innodb_flush_neighbors"`
	InnodbBufPoolInsts      uint64 `db:"@@innodb_buffer_pool_instances"`
	InnodbBufPoolChunkSize  uint64 `db:"@@innodb_buffer_pool_chunk_size"`
	TableOpenCacheInsts     uint64 `db:"@@table_open_cache_instances"`
	InnodbIOCapacityMax     uint64 `db:"@@innodb_io_capacity_max"`
	InnodbIOCapacity        uint64 `db:"@@innodb_io_capacity"`
	LargePages              bool   `db:"@@large_pages"`
	InnodbPageSize          uint16 `db:"@@innodb_page_size"`
	InnodbChecksumAlgo      string `db:"@@innodb_checksum_algorithm"`
	InnodbDoubleWrite       string `db:"@@innodb_doublewrite"`
	InnodbUseNativeAIO      bool   `db:"@@innodb_use_native_aio"`
	InnodbLogWriteAheadSize uint64 `db:"@@innodb_log_write_ahead_size"`
}

// GlobalStatus structure of global status
type GlobalStatus struct {
	OpenTables uint64 `db:"Open_tables"`
}

// GlobalRow structure of SHOW GLOBAL command
type GlobalRow struct {
	Name  string `db:"Variable_name"`
	Value string `db:"Value"`
}

// GetGlobalVariables reads global variables from db
func (e *Executor) GetGlobalVariables() (gvs GlobalVariables, err error) {
	rows := make([]GlobalRow, 0)

	err = e.Select(&rows, "SHOW VARIABLES LIKE 'query_cache_size';")
	if err != nil {
		return
	}

	queryCacheSizeQuery := "0 AS query_cache_size"
	for _, row := range rows {
		if row.Name == "query_cache_size" {
			queryCacheSizeQuery = "@@query_cache_size AS query_cache_size"
			break
		}
	}

	query := fmt.Sprintf(`
SELECT @@innodb_buffer_pool_size, @@innodb_log_buffer_size, @@key_buffer_size, %s,
	@@max_connections, @@join_buffer_size, @@read_buffer_size, @@read_rnd_buffer_size,
	@@sort_buffer_size, @@tmp_table_size, @@table_open_cache, @@table_definition_cache,
	@@innodb_log_file_size, @@innodb_flush_neighbors, @@innodb_buffer_pool_instances,
	@@innodb_io_capacity_max, @@innodb_io_capacity, @@innodb_buffer_pool_chunk_size,
	@@innodb_log_files_in_group, @@large_pages, @@table_open_cache_instances,
	@@innodb_page_size, @@innodb_checksum_algorithm, @@innodb_doublewrite,
	@@innodb_use_native_aio, @@innodb_log_write_ahead_size;
`, queryCacheSizeQuery)

	err = e.Get(&gvs, query)
	return
}

// MaxMemoryUsage returns the maximum memory usage
func (gvs GlobalVariables) MaxMemoryUsage() uint64 {
	return gvs.InnodbBufferPoolSize + gvs.InnodbLogBufferSize + gvs.KeyBufSize +
		gvs.QueryCacheSize + gvs.MaxConnections*(gvs.JoinBufferSize+gvs.ReadBufferSize+gvs.ReadRNDBufferSize+
		gvs.SortBufferSize+gvs.TmpTableSize)
}
