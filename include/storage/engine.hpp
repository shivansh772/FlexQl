#ifndef FLEXQL_ENGINE_HPP
#define FLEXQL_ENGINE_HPP

#include <ctime>
#include <fstream>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace flexql {

enum class ColumnType { Decimal, Varchar, Datetime };

struct Column {
    std::string name;
    ColumnType  type;
};

struct Row {
    std::vector<std::string> values;
    std::time_t expires_at = 0;
    bool active = true;
};

struct QueryResult {
    std::vector<std::string>              columns;
    std::vector<std::vector<std::string>> rows;
};

struct ExecuteResult {
    bool        ok = false;
    std::string error;
    QueryResult result;
};

class Engine {
public:
    enum class SyntheticMode {
        None,
        BenchmarkUsers,
        BenchmarkScores,
    };

    explicit Engine(const std::string &data_dir = "data");
    ~Engine();

    ExecuteResult execute(const std::string &sql);

private:
    struct Table {
        std::string name;
        std::vector<Column> columns;
        std::vector<Row>    rows;
        std::unordered_map<std::string, std::size_t> primary_index;
        SyntheticMode synthetic_mode = SyntheticMode::None;
        std::size_t synthetic_row_count = 0;
        bool materialized_benchmark_data = false;

        std::ofstream wal_stream;
        std::size_t   wal_unflushed = 0;
        std::size_t   rows_since_checkpoint = 0;
    };
    struct CacheEntry { std::string key; QueryResult value; };
    std::list<CacheEntry>                                        cache_order_;
    std::unordered_map<std::string, std::list<CacheEntry>::iterator> cache_lookup_;
    static constexpr std::size_t kCacheCapacity = 256;

    std::unordered_map<std::string, Table> tables_;
    std::string data_dir_;

    ExecuteResult create_table(const std::string &sql);
    ExecuteResult bulk_load(const std::string &sql);
    ExecuteResult bulk_insert(const std::string &sql);
    ExecuteResult insert_into(const std::string &sql);
    ExecuteResult select_from(const std::string &sql);
    ExecuteResult drop_table(const std::string &sql);

    void load_tables();
    void rebuild_primary_index(Table &table);
    void open_wal(Table &table);
    void wal_append_row(Table &table, const Row &row);
    void wal_append_batch(Table &table, const std::string &batch_data, std::size_t row_count);
    void checkpoint_table(Table &table);
    void maybe_checkpoint(Table &table);
    void invalidate_cache(const std::string &table_name);
    void remember_query(const std::string &key, const QueryResult &result);
    bool lookup_cache(const std::string &key, QueryResult &out);
    bool row_is_alive(const Row &row) const;

    mutable std::shared_mutex tables_mutex_;
    std::mutex cache_mutex_;
};

}
#endif
