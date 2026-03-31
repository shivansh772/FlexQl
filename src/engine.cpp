#include "engine.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace flexql {

namespace {

std::string trim(const std::string &s) {
    std::size_t a = 0, b = s.size();
    while (a < b && std::isspace((unsigned char)s[a])) ++a;
    while (b > a && std::isspace((unsigned char)s[b-1])) --b;
    return s.substr(a, b - a);
}

std::string upper(std::string s) {
    for (char &c : s) c = (char)std::toupper((unsigned char)c);
    return s;
}

std::string without_semicolon(std::string s) {
    s = trim(s);
    if (!s.empty() && s.back() == ';') s.pop_back();
    return trim(s);
}

std::string normalize_id(std::string s) { return upper(trim(s)); }

bool starts_with(const std::string &s, const char *kw) {
    std::size_t n = std::strlen(kw);
    return s.size() >= n && upper(s.substr(0, n)) == kw;
}

std::vector<std::string> split_csv(const std::string &s) {
    std::vector<std::string> out;
    std::string cur;
    bool in_q = false;
    for (std::size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'' && (i == 0 || s[i-1] != '\\')) { in_q = !in_q; cur += c; continue; }
        if (c == ',' && !in_q) { out.push_back(trim(cur)); cur.clear(); continue; }
        cur += c;
    }
    if (!cur.empty() || !out.empty()) out.push_back(trim(cur));
    return out;
}

std::vector<std::string> split_value_groups(const std::string &s) {
    std::vector<std::string> groups;
    std::string cur;
    bool in_q = false;
    int depth = 0;
    for (std::size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'' && (i == 0 || s[i-1] != '\\')) in_q = !in_q;
        if (!in_q && c == '(') {
            if (depth == 0) cur.clear(); else cur += c;
            ++depth; continue;
        }
        if (!in_q && c == ')') {
            --depth;
            if (depth == 0) { groups.push_back(trim(cur)); cur.clear(); }
            else cur += c;
            continue;
        }
        if (depth > 0) cur += c;
    }
    return groups;
}

std::string unquote(std::string v) {
    v = trim(v);
    if (v.size() >= 2 && v.front() == '\'' && v.back() == '\'')
        v = v.substr(1, v.size() - 2);
    std::string out; out.reserve(v.size());
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (v[i] == '\\' && i+1 < v.size()) { ++i; }
        out += v[i];
    }
    return out;
}

bool try_double(const std::string &s, double &val) {
    char *end = nullptr;
    val = std::strtod(s.c_str(), &end);
    return end != s.c_str() && end && *end == '\0';
}

bool try_datetime(const std::string &s, std::tm &tm) {
    std::istringstream iss(s);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    return !iss.fail() && iss.peek() == std::char_traits<char>::eof();
}

bool compare_values(const std::string &lhs, const std::string &rhs, const std::string &op) {
    double l = 0, r = 0;
    bool num = try_double(lhs, l) && try_double(rhs, r);
    if (num) {
        if (op == "=")  return l == r;
        if (op == "!=") return l != r;
        if (op == "<")  return l <  r;
        if (op == "<=") return l <= r;
        if (op == ">")  return l >  r;
        if (op == ">=") return l >= r;
    }
    if (op == "=")  return lhs == rhs;
    if (op == "!=") return lhs != rhs;
    if (op == "<")  return lhs <  rhs;
    if (op == "<=") return lhs <= rhs;
    if (op == ">")  return lhs >  rhs;
    if (op == ">=") return lhs >= rhs;
    return false;
}

struct Condition { std::string column, op, value; };

Condition parse_condition(const std::string &s) {
    for (const char *op : {"<=", ">=", "!=", "=", "<", ">"}) {
        std::size_t p = s.find(op);
        if (p != std::string::npos)
            return {trim(s.substr(0, p)), op, trim(s.substr(p + std::strlen(op)))};
    }
    throw std::runtime_error("Unsupported WHERE/ON condition: " + s);
}

std::size_t find_col_idx(const std::vector<std::string> &cols, const std::string &name) {
    std::string want = normalize_id(name);
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (normalize_id(cols[i]) == want) return i;
        std::size_t dot = cols[i].find('.');
        if (dot != std::string::npos && normalize_id(cols[i].substr(dot+1)) == want) return i;
    }
    throw std::runtime_error("Unknown column: " + name);
}

std::string esc(const std::string &v) {
    std::string out; out.reserve(v.size());
    for (char c : v) { if (c == '\\' || c == '|') out += '\\'; out += c; }
    return out;
}

std::vector<std::string> split_escaped(const std::string &s, char delim) {
    std::vector<std::string> parts;
    std::string cur;
    bool escaped = false;
    for (char c : s) {
        if (escaped) { cur += c; escaped = false; continue; }
        if (c == '\\') { escaped = true; continue; }
        if (c == delim) { parts.push_back(cur); cur.clear(); continue; }
        cur += c;
    }
    parts.push_back(cur);
    return parts;
}

std::string serialize_row(const Row &row) {
    std::ostringstream out;
    out << (row.active ? '1' : '0') << '|'
        << static_cast<long long>(row.expires_at) << '|'
        << row.values.size();
    for (const std::string &v : row.values) {
        out << '|' << esc(v);
    }
    return out.str();
}

bool parse_row_line(const std::string &line, Row &row) {
    auto parts = split_escaped(line, '|');
    if (parts.size() < 3) {
        return false;
    }

    row.active = (parts[0] == "1");
    row.expires_at = static_cast<std::time_t>(std::stoll(parts[1]));
    const std::size_t nfields = static_cast<std::size_t>(std::stoul(parts[2]));
    if (parts.size() < 3 + nfields) {
        return false;
    }

    row.values.clear();
    row.values.reserve(nfields);
    for (std::size_t i = 0; i < nfields; ++i) {
        row.values.push_back(parts[3 + i]);
    }
    return true;
}

}

Engine::Engine(const std::string &data_dir) : data_dir_(data_dir) {
    load_tables();
}

Engine::~Engine() {
    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    for (auto &entry : tables_) {
        Table &table = entry.second;
        table.wal_stream.flush();
        checkpoint_table(table);
    }
}

bool Engine::row_is_alive(const Row &row) const {
    if (!row.active) return false;
    if (row.expires_at == 0) return true;
    return row.expires_at > std::time(nullptr);
}
void Engine::open_wal(Table &table) {
    namespace fs = std::filesystem;
    fs::path wal_path = fs::path(data_dir_) / "tables" / (table.name + ".wal");
    table.wal_stream.open(wal_path, std::ios::app | std::ios::binary);
    if (!table.wal_stream)
        throw std::runtime_error("Cannot open WAL for table: " + table.name);
}

void Engine::wal_append_row(Table &table, const Row &row) {
    table.wal_stream << serialize_row(row) << '\n';
    ++table.wal_unflushed;
    ++table.rows_since_checkpoint;
    if (table.wal_unflushed >= 50000) {
        table.wal_stream.flush();
        table.wal_unflushed = 0;
    }
}

void Engine::checkpoint_table(Table &table) {
    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);

    const fs::path data_path = table_dir / (table.name + ".data");
    const fs::path temp_path = table_dir / (table.name + ".data.tmp");
    {
        std::ofstream snapshot(temp_path, std::ios::trunc | std::ios::binary);
        if (!snapshot) {
            throw std::runtime_error("Cannot write checkpoint for table: " + table.name);
        }
        for (const Row &row : table.rows) {
            snapshot << serialize_row(row) << '\n';
        }
        snapshot.flush();
        if (!snapshot) {
            throw std::runtime_error("Failed to flush checkpoint for table: " + table.name);
        }
    }

    std::error_code ec;
    fs::remove(data_path, ec);
    ec.clear();
    fs::rename(temp_path, data_path, ec);
    if (ec) {
        fs::remove(temp_path);
        throw std::runtime_error("Failed to publish checkpoint for table: " + table.name);
    }

    if (table.wal_stream.is_open()) {
        table.wal_stream.flush();
        table.wal_stream.close();
    }

    std::ofstream truncate_wal(table_dir / (table.name + ".wal"), std::ios::trunc | std::ios::binary);
    if (!truncate_wal) {
        throw std::runtime_error("Failed to truncate WAL for table: " + table.name);
    }
    truncate_wal.close();

    open_wal(table);
    table.wal_unflushed = 0;
    table.rows_since_checkpoint = 0;
}

void Engine::maybe_checkpoint(Table &table) {
    static constexpr std::size_t kCheckpointInterval = 250000;
    if (table.rows_since_checkpoint >= kCheckpointInterval) {
        checkpoint_table(table);
    }
}

void Engine::load_tables() {
    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);

    for (const fs::directory_entry &entry : fs::directory_iterator(table_dir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".schema") continue;

        std::ifstream schema_f(entry.path());
        if (!schema_f) continue;

        Table table;
        table.name = normalize_id(entry.path().stem().string());

        std::string line;
        while (std::getline(schema_f, line)) {
            line = trim(line);
            if (line.empty()) continue;
            auto parts = split_escaped(line, '|');
            if (parts.size() < 2) continue;
            table.columns.push_back({
                normalize_id(parts[0]),
                upper(parts[1]) == "DECIMAL" ? ColumnType::Decimal :
                    (upper(parts[1]) == "DATETIME" ? ColumnType::Datetime : ColumnType::Varchar)
            });
        }

        const fs::path data_path = table_dir / (table.name + ".data");
        std::ifstream data_f(data_path);
        while (data_f && std::getline(data_f, line)) {
            if (trim(line).empty()) continue;
            Row row;
            if (!parse_row_line(line, row)) continue;
            table.rows.push_back(std::move(row));
        }

        const fs::path wal_path = table_dir / (table.name + ".wal");
        std::ifstream wal_f(wal_path);
        while (wal_f && std::getline(wal_f, line)) {
            if (trim(line).empty()) continue;
            Row row;
            if (!parse_row_line(line, row)) continue;
            table.rows.push_back(std::move(row));
        }

        rebuild_primary_index(table);
        open_wal(table);
        tables_[table.name] = std::move(table);
    }
}

void Engine::rebuild_primary_index(Table &table) {
    table.primary_index.clear();
    for (std::size_t i = 0; i < table.rows.size(); ++i) {
        const Row &row = table.rows[i];
        if (row.active && !row.values.empty())
            table.primary_index[row.values.front()] = i;
    }
}

void Engine::remember_query(const std::string &key, const QueryResult &result) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = cache_lookup_.find(key);
    if (it != cache_lookup_.end()) {
        cache_order_.erase(it->second);
        cache_lookup_.erase(it);
    }
    cache_order_.push_front({key, result});
    cache_lookup_[key] = cache_order_.begin();
    if (cache_order_.size() > kCacheCapacity) {
        cache_lookup_.erase(cache_order_.back().key);
        cache_order_.pop_back();
    }
}

bool Engine::lookup_cache(const std::string &key, QueryResult &out) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = cache_lookup_.find(key);
    if (it == cache_lookup_.end()) return false;
    cache_order_.splice(cache_order_.begin(), cache_order_, it->second);
    out = it->second->value;
    return true;
}

void Engine::invalidate_cache(const std::string &table_name) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    for (auto it = cache_order_.begin(); it != cache_order_.end(); ) {
        if (it->key.find(table_name) != std::string::npos) {
            cache_lookup_.erase(it->key);
            it = cache_order_.erase(it);
        } else ++it;
    }
}

ExecuteResult Engine::execute(const std::string &raw_sql) {
    const std::string sql = without_semicolon(raw_sql);
    if (sql.empty()) return {false, "Empty SQL statement", {}};
    try {
        if (starts_with(sql, "CREATE TABLE")) return create_table(sql);
        if (starts_with(sql, "INSERT INTO"))  return insert_into(sql);
        if (starts_with(sql, "DROP TABLE"))   return drop_table(sql);
        if (starts_with(sql, "SELECT"))       return select_from(sql);
        return {false, "Unsupported SQL command", {}};
    } catch (const std::exception &ex) {
        return {false, ex.what(), {}};
    }
}

ExecuteResult Engine::create_table(const std::string &sql) {
    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    const std::size_t name_start = 12;
    const std::size_t open  = sql.find('(', name_start);
    const std::size_t close = sql.rfind(')');
    if (open == std::string::npos || close == std::string::npos || close <= open)
        throw std::runtime_error("Invalid CREATE TABLE syntax");

    const std::string table_name = normalize_id(sql.substr(name_start, open - name_start));
    if (table_name.empty()) throw std::runtime_error("Missing table name");
    if (tables_.count(table_name)) throw std::runtime_error("Table already exists: " + table_name);

    Table table;
    table.name = table_name;

    for (const std::string &def : split_csv(sql.substr(open + 1, close - open - 1))) {
        std::istringstream iss(def);
        std::string col_name, type_str;
        iss >> col_name >> type_str;
        if (col_name.empty() || type_str.empty())
            throw std::runtime_error("Invalid column definition: " + def);

        std::string utype = upper(type_str);
        std::size_t paren = utype.find('(');
        if (paren != std::string::npos) utype = utype.substr(0, paren);

        ColumnType ct;
        if (utype == "DECIMAL" || utype == "INT") ct = ColumnType::Decimal;
        else if (utype == "VARCHAR" || utype == "TEXT") ct = ColumnType::Varchar;
        else if (utype == "DATETIME") ct = ColumnType::Datetime;
        else throw std::runtime_error("Unsupported column type: " + type_str);

        table.columns.push_back({normalize_id(col_name), ct});
    }

    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);
    const fs::path schema_path = table_dir / (table_name + ".schema");
    {
        std::ofstream sf(schema_path, std::ios::trunc);
        for (const Column &c : table.columns)
            sf << esc(c.name) << '|'
               << (c.type == ColumnType::Decimal ? "DECIMAL" :
                   (c.type == ColumnType::Datetime ? "DATETIME" : "VARCHAR")) << '\n';
    }

    open_wal(table);
    tables_[table_name] = std::move(table);
    return {true, {}, {}};
}

ExecuteResult Engine::insert_into(const std::string &sql) {
    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    const std::string sql_up = upper(sql);
    const std::size_t into_pos   = sql_up.find("INSERT INTO");
    const std::size_t values_pos = sql_up.find("VALUES", into_pos + 11);
    if (values_pos == std::string::npos)
        throw std::runtime_error("Invalid INSERT syntax");

    const std::string table_name = normalize_id(sql.substr(into_pos + 11, values_pos - (into_pos + 11)));
    auto tit = tables_.find(table_name);
    if (tit == tables_.end()) throw std::runtime_error("Unknown table: " + table_name);
    Table &table = tit->second;

    int expires_col = -1;
    for (int i = 0; i < (int)table.columns.size(); ++i)
        if (table.columns[i].name == "EXPIRES_AT") { expires_col = i; break; }

    const std::string values_clause = trim(sql.substr(values_pos + 6));
    const std::vector<std::string> groups = split_value_groups(values_clause);
    if (groups.empty()) throw std::runtime_error("INSERT requires at least one row");

    table.rows.reserve(table.rows.size() + groups.size());
    table.primary_index.reserve(table.primary_index.size() + groups.size());

    for (const std::string &group : groups) {
        const std::vector<std::string> raw = split_csv(group);
        if (raw.size() != table.columns.size())
            throw std::runtime_error("INSERT column count mismatch");

        Row row;
        row.expires_at = 0;
        row.values.reserve(raw.size());

        for (std::size_t i = 0; i < raw.size(); ++i) {
            std::string val = unquote(raw[i]);
            if (table.columns[i].type == ColumnType::Decimal) {
                double d = 0;
                if (!try_double(val, d))
                    throw std::runtime_error("Expected DECIMAL for column " + table.columns[i].name);
                if ((int)i == expires_col) {
                    long long ts = static_cast<long long>(d);
                    row.expires_at = (ts > 0) ? static_cast<std::time_t>(ts) : 0;
                }
            } else if (table.columns[i].type == ColumnType::Datetime) {
                std::tm tm = {};
                if (!try_datetime(val, tm))
                    throw std::runtime_error(
                        "Expected DATETIME in format YYYY-MM-DD HH:MM:SS for column " + table.columns[i].name);
            }
            row.values.push_back(std::move(val));
        }

        const std::string &pk = row.values.front();
        auto pk_it = table.primary_index.find(pk);
        if (pk_it != table.primary_index.end()) {
            const Row &existing = table.rows[pk_it->second];
            if (existing.active)
                throw std::runtime_error("Duplicate primary key: " + pk);
        }

        wal_append_row(table, row);
        table.primary_index[pk] = table.rows.size();
        table.rows.push_back(std::move(row));
    }

    maybe_checkpoint(table);
    invalidate_cache(table_name);
    return {true, {}, {}};
}

ExecuteResult Engine::drop_table(const std::string &sql) {
    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    const std::string table_name = normalize_id(sql.substr(10));
    if (table_name.empty()) throw std::runtime_error("Missing table name");

    auto it = tables_.find(table_name);
    if (it == tables_.end()) throw std::runtime_error("Unknown table: " + table_name);

    it->second.wal_stream.flush();
    it->second.wal_stream.close();
    tables_.erase(it);

    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    std::error_code ec;
    fs::remove(table_dir / (table_name + ".schema"), ec);
    if (ec) {
        throw std::runtime_error("Failed to remove schema for table: " + table_name);
    }
    ec.clear();
    fs::remove(table_dir / (table_name + ".wal"), ec);
    if (ec) {
        throw std::runtime_error("Failed to remove WAL for table: " + table_name);
    }
    ec.clear();
    fs::remove(table_dir / (table_name + ".data"), ec);
    if (ec) {
        throw std::runtime_error("Failed to remove checkpoint for table: " + table_name);
    }

    invalidate_cache(table_name);
    return {true, {}, {}};
}

ExecuteResult Engine::select_from(const std::string &sql) {
    QueryResult cached;
    if (lookup_cache(sql, cached)) return {true, {}, cached};

    std::shared_lock<std::shared_mutex> tables_lock(tables_mutex_);

    const std::string sql_up = upper(sql);
    const std::size_t from_pos = sql_up.find(" FROM ");
    if (from_pos == std::string::npos) throw std::runtime_error("Invalid SELECT syntax");

    const std::string projection = trim(sql.substr(6, from_pos - 6));
    const std::string rest       = trim(sql.substr(from_pos + 6));
    const std::string rest_up    = upper(rest);

    const std::size_t join_pos  = rest_up.find(" INNER JOIN ");
    const std::size_t where_pos = rest_up.find(" WHERE ");

    QueryResult result;

    if (join_pos == std::string::npos) {
        const std::size_t tbl_end = (where_pos == std::string::npos) ? rest.size() : where_pos;
        const std::string table_name = normalize_id(rest.substr(0, tbl_end));
        auto tit = tables_.find(table_name);
        if (tit == tables_.end()) throw std::runtime_error("Unknown table: " + table_name);
        Table &table = tit->second;

        std::vector<std::string> all_cols;
        all_cols.reserve(table.columns.size());
        for (const Column &c : table.columns) all_cols.push_back(c.name);

        std::vector<std::size_t> sel_idx;
        if (projection == "*") {
            result.columns = all_cols;
            for (std::size_t i = 0; i < all_cols.size(); ++i) sel_idx.push_back(i);
        } else {
            for (const std::string &p : split_csv(projection)) {
                std::size_t idx = find_col_idx(all_cols, p);
                sel_idx.push_back(idx);
                result.columns.push_back(all_cols[idx]);
            }
        }

        bool use_index = false;
        Condition cond;
        std::size_t filter_idx = 0;
        std::string filter_value;
        if (where_pos != std::string::npos) {
            cond = parse_condition(rest.substr(where_pos + 7));
            filter_idx = find_col_idx(all_cols, cond.column);
            filter_value = unquote(cond.value);
            if (filter_idx == 0 && cond.op == "=")
                use_index = true;
        }

        auto emit = [&](const Row &row) {
            std::vector<std::string> out; out.reserve(sel_idx.size());
            for (std::size_t i : sel_idx) out.push_back(row.values[i]);
            result.rows.push_back(std::move(out));
        };

        if (use_index) {
            auto it = table.primary_index.find(filter_value);
            if (it != table.primary_index.end()) {
                const Row &row = table.rows[it->second];
                if (row_is_alive(row)) emit(row);
            }
        } else {
            for (const Row &row : table.rows) {
                if (!row_is_alive(row)) continue;
                if (where_pos != std::string::npos) {
                    if (!compare_values(row.values[filter_idx], filter_value, cond.op)) continue;
                }
                emit(row);
            }
        }
    } else {
        const std::string left_name = normalize_id(rest.substr(0, join_pos));
        const std::size_t on_pos    = rest_up.find(" ON ", join_pos + 12);
        if (on_pos == std::string::npos) throw std::runtime_error("JOIN requires ON condition");
        const std::string right_name = normalize_id(rest.substr(join_pos + 12, on_pos - (join_pos + 12)));
        const std::size_t local_where = rest_up.find(" WHERE ", on_pos + 4);
        const std::string join_cond_text = trim(rest.substr(on_pos + 4,
            local_where == std::string::npos ? rest.size() - (on_pos + 4) : local_where - (on_pos + 4)));
        const Condition join_cond = parse_condition(join_cond_text);

        auto lit = tables_.find(left_name);
        auto rit = tables_.find(right_name);
        if (lit == tables_.end()) throw std::runtime_error("Unknown table: " + left_name);
        if (rit == tables_.end()) throw std::runtime_error("Unknown table: " + right_name);
        Table &left  = lit->second;
        Table &right = rit->second;

        std::vector<std::string> all_cols;
        for (const Column &c : left.columns)  all_cols.push_back(left_name  + "." + c.name);
        for (const Column &c : right.columns) all_cols.push_back(right_name + "." + c.name);

        std::vector<std::size_t> sel_idx;
        if (projection == "*") {
            result.columns = all_cols;
            for (std::size_t i = 0; i < all_cols.size(); ++i) sel_idx.push_back(i);
        } else {
            for (const std::string &p : split_csv(projection)) {
                std::size_t idx = find_col_idx(all_cols, p);
                sel_idx.push_back(idx);
                result.columns.push_back(all_cols[idx]);
            }
        }

        std::size_t l_join_idx = find_col_idx(all_cols, join_cond.column);
        std::size_t r_join_idx = find_col_idx(all_cols, unquote(join_cond.value));

        bool has_where = (local_where != std::string::npos);
        Condition filter_cond;
        std::size_t filter_idx = 0;
        if (has_where) {
            filter_cond = parse_condition(rest.substr(local_where + 7));
            filter_idx  = find_col_idx(all_cols, filter_cond.column);
        }

        const std::string where_value = has_where ? unquote(filter_cond.value) : std::string();
        const std::size_t left_col_count = left.columns.size();
        auto joined_value = [&](const Row &lr, const Row &rr, std::size_t idx) -> const std::string & {
            return (idx < left_col_count) ? lr.values[idx] : rr.values[idx - left_col_count];
        };

        auto emit_joined = [&](const Row &lr, const Row &rr) {
            if (has_where) {
                const std::string &filter_cell = joined_value(lr, rr, filter_idx);
                if (!compare_values(filter_cell, where_value, filter_cond.op)) {
                    return;
                }
            }

            std::vector<std::string> projected;
            projected.reserve(sel_idx.size());
            for (std::size_t idx : sel_idx) {
                if (idx < left_col_count) {
                    projected.push_back(lr.values[idx]);
                } else {
                    projected.push_back(rr.values[idx - left_col_count]);
                }
            }
            result.rows.push_back(std::move(projected));
        };

        const bool can_use_hash_join = join_cond.op == "=" &&
            l_join_idx < left_col_count &&
            r_join_idx >= left_col_count;

        if (can_use_hash_join) {
            std::unordered_map<std::string, std::vector<const Row *>> right_lookup;
            right_lookup.reserve(right.rows.size());
            for (const Row &rr : right.rows) {
                if (!row_is_alive(rr)) continue;
                right_lookup[rr.values[r_join_idx - left_col_count]].push_back(&rr);
            }

            for (const Row &lr : left.rows) {
                if (!row_is_alive(lr)) continue;
                auto it = right_lookup.find(lr.values[l_join_idx]);
                if (it == right_lookup.end()) continue;
                for (const Row *rr : it->second) {
                    emit_joined(lr, *rr);
                }
            }
        } else {
            for (const Row &lr : left.rows) {
                if (!row_is_alive(lr)) continue;
                for (const Row &rr : right.rows) {
                    if (!row_is_alive(rr)) continue;
                    if (!compare_values(joined_value(lr, rr, l_join_idx), joined_value(lr, rr, r_join_idx), join_cond.op)) continue;
                    emit_joined(lr, rr);
                }
            }
        }
    }

    remember_query(sql, result);
    return {true, {}, result};
}

}
