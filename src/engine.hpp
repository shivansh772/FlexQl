#ifndef FLEXQL_ENGINE_HPP
#define FLEXQL_ENGINE_HPP

#include <ctime>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

namespace flexql {

enum class ColumnType {
    Decimal,
    Varchar
};

struct Column {
    std::string name;
    ColumnType type;
};

struct Row {
    std::vector<std::string> values;
    std::time_t expires_at;
    bool active = true;
};

struct QueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

struct ExecuteResult {
    bool ok = false;
    std::string error;
    QueryResult result;
};

class Engine {
  public:
    Engine();
    ExecuteResult execute(const std::string &sql);

  private:
    struct Table {
        std::string name;
        std::vector<Column> columns;
        std::vector<Row> rows;
        std::unordered_map<std::string, std::size_t> primary_index;
    };

    struct CacheEntry {
        std::string key;
        QueryResult value;
    };

    std::unordered_map<std::string, Table> tables_;
    std::list<CacheEntry> cache_order_;
    std::unordered_map<std::string, std::list<CacheEntry>::iterator> cache_lookup_;

    static constexpr std::size_t kCacheCapacity = 32;
    static constexpr std::time_t kDefaultTtlSeconds = 24 * 60 * 60;
    const std::string data_dir_ = "data";

    ExecuteResult create_table(const std::string &sql);
    ExecuteResult insert_into(const std::string &sql);
    ExecuteResult select_from(const std::string &sql);

    void trim_expired(Table &table);
    void remember_query(const std::string &sql, const QueryResult &result);
    void load_tables();
    void rebuild_primary_index(Table &table);
    void persist_table(const Table &table) const;
};

}  // namespace flexql

#endif
