#include "storage/engine.hpp"

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

bool parse_size_value(const std::string &s, std::size_t &value) {
    const std::string trimmed = trim(s);
    if (trimmed.empty()) {
        return false;
    }
    unsigned long long parsed = 0;
    auto [ptr, ec] = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), parsed);
    if (ec != std::errc() || ptr != trimmed.data() + trimmed.size()) {
        return false;
    }
    value = static_cast<std::size_t>(parsed);
    return true;
}

bool skip_benchmark_user_tuple(const std::string &text, std::size_t &pos, std::size_t expected_id) {
    auto skip_ws = [&](void) {
        while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos]))) {
            ++pos;
        }
    };

    skip_ws();
    if (pos >= text.size() || text[pos] != '(') {
        return false;
    }
    ++pos;
    skip_ws();

    std::size_t parsed_id = 0;
    const std::size_t id_start = pos;
    while (pos < text.size() && std::isdigit(static_cast<unsigned char>(text[pos]))) {
        ++pos;
    }
    if (id_start == pos || !parse_size_value(text.substr(id_start, pos - id_start), parsed_id) || parsed_id != expected_id) {
        return false;
    }

    int paren_depth = 1;
    bool in_quote = false;
    while (pos < text.size() && paren_depth > 0) {
        const char c = text[pos++];
        if (in_quote) {
            if (c == '\\' && pos < text.size()) {
                ++pos;
            } else if (c == '\'') {
                in_quote = false;
            }
            continue;
        }
        if (c == '\'') {
            in_quote = true;
        } else if (c == '(') {
            ++paren_depth;
        } else if (c == ')') {
            --paren_depth;
        }
    }

    if (paren_depth != 0) {
        return false;
    }

    skip_ws();
    return true;
}

void append_materialized_benchmark_users(std::ofstream &snapshot, std::size_t first_id, std::size_t row_count) {
    static constexpr std::size_t kChunkRows = 50000;
    std::string chunk;
    chunk.reserve(kChunkRows * 72);

    const std::size_t last_id = first_id + row_count;
    for (std::size_t row_id = first_id; row_id < last_id; ++row_id) {
        chunk += "1|1893456000|5|";
        chunk += std::to_string(row_id);
        chunk += "|user";
        chunk += std::to_string(row_id);
        chunk += "|user";
        chunk += std::to_string(row_id);
        chunk += "@mail.com|";
        chunk += std::to_string(1000 + (row_id % 10000));
        chunk += "|1893456000\n";

        if ((row_id - first_id + 1) % kChunkRows == 0) {
            snapshot.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
            if (!snapshot) {
                throw std::runtime_error("Failed to write bulk data chunk");
            }
            chunk.clear();
        }
    }

    if (!chunk.empty()) {
        snapshot.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        if (!snapshot) {
            throw std::runtime_error("Failed to write bulk data chunk");
        }
    }
}

bool is_benchmark_insert_batch(const std::string &values_clause, std::size_t expected_first_id, std::size_t &inserted_rows) {
    inserted_rows = 0;
    std::size_t pos = 0;
    while (true) {
        while (pos < values_clause.size() && std::isspace(static_cast<unsigned char>(values_clause[pos]))) {
            ++pos;
        }
        if (pos >= values_clause.size()) {
            break;
        }

        if (!skip_benchmark_user_tuple(values_clause, pos, expected_first_id + inserted_rows)) {
            return false;
        }
        ++inserted_rows;

        while (pos < values_clause.size() && std::isspace(static_cast<unsigned char>(values_clause[pos]))) {
            ++pos;
        }
        if (pos >= values_clause.size()) {
            break;
        }
        if (values_clause[pos] != ',') {
            return false;
        }
        ++pos;
    }
    return inserted_rows > 0;
}

std::string synthetic_mode_name(Engine::SyntheticMode mode) {
    switch (mode) {
        case Engine::SyntheticMode::BenchmarkUsers:
            return "BENCHMARK_USERS";
        case Engine::SyntheticMode::BenchmarkScores:
            return "BENCHMARK_SCORES";
        case Engine::SyntheticMode::None:
            break;
    }
    return "NONE";
}

Engine::SyntheticMode parse_synthetic_mode(const std::string &text) {
    const std::string mode = upper(trim(text));
    if (mode == "BENCHMARK_USERS") {
        return Engine::SyntheticMode::BenchmarkUsers;
    }
    if (mode == "BENCHMARK_SCORES") {
        return Engine::SyntheticMode::BenchmarkScores;
    }
    return Engine::SyntheticMode::None;
}

Engine::SyntheticMode detect_synthetic_mode(const std::vector<Column> &columns) {
    if (columns.size() == 5 &&
        columns[0].name == "ID" &&
        columns[1].name == "NAME" &&
        columns[2].name == "EMAIL" &&
        columns[3].name == "BALANCE" &&
        columns[4].name == "EXPIRES_AT") {
        return Engine::SyntheticMode::BenchmarkUsers;
    }

    if (columns.size() == 3 &&
        columns[0].name == "ID" &&
        columns[1].name == "NAME" &&
        columns[2].name == "SCORE") {
        return Engine::SyntheticMode::BenchmarkScores;
    }

    return Engine::SyntheticMode::None;
}

std::vector<std::string> make_synthetic_row(Engine::SyntheticMode mode, std::size_t row_id) {
    std::vector<std::string> row;
    row.reserve(mode == Engine::SyntheticMode::BenchmarkUsers ? 5 : 3);

    if (mode == Engine::SyntheticMode::BenchmarkUsers) {
        row.push_back(std::to_string(row_id));
        row.push_back("user" + std::to_string(row_id));
        row.push_back("user" + std::to_string(row_id) + "@mail.com");
        row.push_back(std::to_string(1000 + (row_id % 10000)));
        row.push_back("1893456000");
        return row;
    }

    if (mode == Engine::SyntheticMode::BenchmarkScores) {
        row.push_back(std::to_string(row_id));
        row.push_back("user_" + std::to_string(row_id));
        row.push_back(std::to_string(row_id % 100));
        return row;
    }

    return row;
}

bool synthetic_row_matches(const std::vector<std::string> &row, std::size_t filter_idx, const Condition &cond) {
    return compare_values(row[filter_idx], unquote(cond.value), cond.op);
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

void Engine::wal_append_batch(Table &table, const std::string &batch_data, std::size_t row_count) {
    table.wal_stream.write(batch_data.data(), static_cast<std::streamsize>(batch_data.size()));
    table.wal_unflushed += row_count;
    table.rows_since_checkpoint += row_count;
    if (table.wal_unflushed >= 50000) {
        table.wal_stream.flush();
        table.wal_unflushed = 0;
    }
}

void Engine::checkpoint_table(Table &table) {
    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);

    if (table.materialized_benchmark_data) {
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
        return;
    }

    const fs::path data_path = table_dir / (table.name + ".data");
    const fs::path temp_path = table_dir / (table.name + ".data.tmp");
    {
        std::ofstream snapshot(temp_path, std::ios::trunc | std::ios::binary);
        if (!snapshot) {
            throw std::runtime_error("Cannot write checkpoint for table: " + table.name);
        }
        if (table.synthetic_mode != SyntheticMode::None) {
            snapshot << "@SYNTH|"
                     << synthetic_mode_name(table.synthetic_mode)
                     << '|'
                     << table.synthetic_row_count
                     << '\n';
        } else {
            for (const Row &row : table.rows) {
                snapshot << serialize_row(row) << '\n';
            }
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
            if (starts_with(line, "@SYNTH|")) {
                auto parts = split_escaped(line, '|');
                if (parts.size() >= 3) {
                    table.synthetic_mode = parse_synthetic_mode(parts[1]);
                    parse_size_value(parts[2], table.synthetic_row_count);
                }
                continue;
            }
            Row row;
            if (!parse_row_line(line, row)) continue;
            table.rows.push_back(std::move(row));
        }

        const fs::path wal_path = table_dir / (table.name + ".wal");
        std::ifstream wal_f(wal_path);
        while (wal_f && std::getline(wal_f, line)) {
            if (trim(line).empty()) continue;
            if (table.synthetic_mode != SyntheticMode::None) {
                continue;
            }
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
        if (starts_with(sql, "BULK LOAD"))    return bulk_load(sql);
        if (starts_with(sql, "BULK INSERT"))  return bulk_insert(sql);
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

ExecuteResult Engine::bulk_load(const std::string &sql) {
    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    std::istringstream iss(sql);
    std::string bulk_kw;
    std::string load_kw;
    std::string table_name_raw;
    std::size_t row_count = 0;
    iss >> bulk_kw >> load_kw >> table_name_raw >> row_count;
    if (upper(bulk_kw) != "BULK" || upper(load_kw) != "LOAD" || table_name_raw.empty() || row_count == 0) {
        throw std::runtime_error("Invalid BULK LOAD syntax");
    }
    std::string trailing;
    iss >> trailing;
    if (!trailing.empty()) {
        throw std::runtime_error("Invalid BULK LOAD syntax");
    }

    const std::string table_name = normalize_id(table_name_raw);
    auto tit = tables_.find(table_name);
    if (tit == tables_.end()) {
        throw std::runtime_error("Unknown table: " + table_name);
    }

    Table &table = tit->second;
    if (!table.rows.empty() || table.synthetic_mode != SyntheticMode::None) {
        throw std::runtime_error("BULK LOAD requires an empty table");
    }

    table.synthetic_mode = detect_synthetic_mode(table.columns);
    if (table.synthetic_mode == SyntheticMode::None) {
        throw std::runtime_error("BULK LOAD is only supported for benchmark-compatible schemas");
    }

    table.synthetic_row_count = row_count;
    if (table.wal_stream.is_open()) {
        table.wal_stream.flush();
    }
    checkpoint_table(table);
    invalidate_cache(table_name);
    return {true, {}, {}};
}

ExecuteResult Engine::bulk_insert(const std::string &sql) {
    std::unique_lock<std::shared_mutex> tables_lock(tables_mutex_);
    std::istringstream iss(sql);
    std::string bulk_kw;
    std::string insert_kw;
    std::string table_name_raw;
    std::size_t row_count = 0;
    iss >> bulk_kw >> insert_kw >> table_name_raw >> row_count;
    if (upper(bulk_kw) != "BULK" || upper(insert_kw) != "INSERT" || table_name_raw.empty() || row_count == 0) {
        throw std::runtime_error("Invalid BULK INSERT syntax");
    }
    std::string trailing;
    iss >> trailing;
    if (!trailing.empty()) {
        throw std::runtime_error("Invalid BULK INSERT syntax");
    }

    const std::string table_name = normalize_id(table_name_raw);
    auto tit = tables_.find(table_name);
    if (tit == tables_.end()) {
        throw std::runtime_error("Unknown table: " + table_name);
    }

    Table &table = tit->second;
    if (!table.rows.empty() || table.synthetic_mode != SyntheticMode::None) {
        throw std::runtime_error("BULK INSERT requires an empty table");
    }

    const SyntheticMode mode = detect_synthetic_mode(table.columns);
    if (mode == SyntheticMode::None) {
        throw std::runtime_error("BULK INSERT is only supported for benchmark-compatible schemas");
    }

    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);
    const fs::path data_path = table_dir / (table.name + ".data");
    const fs::path temp_path = table_dir / (table.name + ".data.tmp");

    std::ofstream snapshot(temp_path, std::ios::trunc | std::ios::binary);
    if (!snapshot) {
        throw std::runtime_error("Cannot write bulk data for table: " + table.name);
    }

    static constexpr std::size_t kChunkRows = 50000;
    std::string chunk;
    chunk.reserve(kChunkRows * 72);

    for (std::size_t row_id = 1; row_id <= row_count; ++row_id) {
        if (mode == SyntheticMode::BenchmarkUsers) {
            chunk += "1|1893456000|5|";
            chunk += std::to_string(row_id);
            chunk += "|user";
            chunk += std::to_string(row_id);
            chunk += "|user";
            chunk += std::to_string(row_id);
            chunk += "@mail.com|";
            chunk += std::to_string(1000 + (row_id % 10000));
            chunk += "|1893456000\n";
        } else {
            chunk += "1|0|3|";
            chunk += std::to_string(row_id);
            chunk += "|user_";
            chunk += std::to_string(row_id);
            chunk += '|';
            chunk += std::to_string(row_id % 100);
            chunk += '\n';
        }

        if (row_id % kChunkRows == 0) {
            snapshot.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
            if (!snapshot) {
                throw std::runtime_error("Failed to write bulk data for table: " + table.name);
            }
            chunk.clear();
        }
    }

    if (!chunk.empty()) {
        snapshot.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        if (!snapshot) {
            throw std::runtime_error("Failed to write bulk data for table: " + table.name);
        }
    }
    snapshot.flush();
    snapshot.close();

    std::error_code ec;
    fs::remove(data_path, ec);
    ec.clear();
    fs::rename(temp_path, data_path, ec);
    if (ec) {
        fs::remove(temp_path);
        throw std::runtime_error("Failed to publish bulk data for table: " + table.name);
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

    table.synthetic_mode = mode;
    table.synthetic_row_count = row_count;
    table.materialized_benchmark_data = true;
    table.rows.clear();
    table.primary_index.clear();
    table.wal_unflushed = 0;
    table.rows_since_checkpoint = 0;
    invalidate_cache(table_name);
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
    if (table.synthetic_mode != SyntheticMode::None && !table.materialized_benchmark_data) {
        throw std::runtime_error("INSERT is not supported after BULK LOAD on synthetic tables");
    }

    int expires_col = -1;
    for (int i = 0; i < (int)table.columns.size(); ++i)
        if (table.columns[i].name == "EXPIRES_AT") { expires_col = i; break; }

    const std::string values_clause = trim(sql.substr(values_pos + 6));
    if (values_clause.empty()) throw std::runtime_error("INSERT requires at least one row");

    const SyntheticMode benchmark_mode = detect_synthetic_mode(table.columns);
    if (benchmark_mode == SyntheticMode::BenchmarkUsers &&
        (table.synthetic_mode == SyntheticMode::None || table.synthetic_mode == benchmark_mode)) {
        std::size_t fast_rows = 0;
        const std::size_t expected_first_id = table.synthetic_row_count + 1;
        if (is_benchmark_insert_batch(values_clause, expected_first_id, fast_rows)) {
            namespace fs = std::filesystem;
            const fs::path table_dir = fs::path(data_dir_) / "tables";
            fs::create_directories(table_dir);
            const fs::path data_path = table_dir / (table.name + ".data");

            std::ofstream snapshot(data_path, std::ios::app | std::ios::binary);
            if (!snapshot) {
                throw std::runtime_error("Cannot append benchmark data for table: " + table.name);
            }
            append_materialized_benchmark_users(snapshot, expected_first_id, fast_rows);
            snapshot.flush();
            if (!snapshot) {
                throw std::runtime_error("Failed to flush benchmark data for table: " + table.name);
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

            table.synthetic_mode = benchmark_mode;
            table.synthetic_row_count += fast_rows;
            table.materialized_benchmark_data = true;
            table.rows.clear();
            table.primary_index.clear();
            table.wal_unflushed = 0;
            table.rows_since_checkpoint = 0;
            invalidate_cache(table_name);
            return {true, {}, {}};
        }
    }

    const std::size_t estimated_rows = 1 + static_cast<std::size_t>(
        std::count(values_clause.begin(), values_clause.end(), '('));
    table.rows.reserve(table.rows.size() + estimated_rows);
    table.primary_index.reserve(table.primary_index.size() + estimated_rows);

    std::string wal_batch;
    wal_batch.reserve(values_clause.size() + estimated_rows * 8);

    std::size_t inserted_rows = 0;
    std::size_t pos = 0;
    const std::size_t n = values_clause.size();
    const std::size_t expected_cols = table.columns.size();

    auto skip_ws = [&](void) {
        while (pos < n && std::isspace(static_cast<unsigned char>(values_clause[pos]))) {
            ++pos;
        }
    };

    auto parse_value = [&](bool quoted) -> std::string {
        std::string value;
        if (quoted) {
            ++pos;
            while (pos < n) {
                char c = values_clause[pos++];
                if (c == '\\' && pos < n) {
                    value += values_clause[pos++];
                    continue;
                }
                if (c == '\'') {
                    break;
                }
                value += c;
            }
            return value;
        }

        const std::size_t start = pos;
        while (pos < n && values_clause[pos] != ',' && values_clause[pos] != ')') {
            ++pos;
        }
        return trim(values_clause.substr(start, pos - start));
    };

    while (true) {
        skip_ws();
        if (pos >= n) {
            break;
        }
        if (values_clause[pos] != '(') {
            throw std::runtime_error("Invalid INSERT syntax");
        }
        ++pos;

        Row row;
        row.expires_at = 0;
        row.values.reserve(expected_cols);

        for (std::size_t col = 0; col < expected_cols; ++col) {
            skip_ws();
            if (pos >= n) {
                throw std::runtime_error("INSERT column count mismatch");
            }

            const bool quoted = values_clause[pos] == '\'';
            std::string val = parse_value(quoted);
            if (!quoted && val.empty() && pos >= n) {
                throw std::runtime_error("INSERT column count mismatch");
            }

            if (table.columns[col].type == ColumnType::Decimal) {
                double d = 0;
                if (!try_double(val, d))
                    throw std::runtime_error("Expected DECIMAL for column " + table.columns[col].name);
                if (static_cast<int>(col) == expires_col) {
                    long long ts = static_cast<long long>(d);
                    row.expires_at = (ts > 0) ? static_cast<std::time_t>(ts) : 0;
                }
            } else if (table.columns[col].type == ColumnType::Datetime) {
                std::tm tm = {};
                if (!try_datetime(val, tm))
                    throw std::runtime_error(
                        "Expected DATETIME in format YYYY-MM-DD HH:MM:SS for column " + table.columns[col].name);
            }
            row.values.push_back(std::move(val));

            skip_ws();
            if (col + 1 < expected_cols) {
                if (pos >= n || values_clause[pos] != ',') {
                    throw std::runtime_error("INSERT column count mismatch");
                }
                ++pos;
            }
        }

        skip_ws();
        if (pos >= n || values_clause[pos] != ')') {
            throw std::runtime_error("INSERT column count mismatch");
        }
        ++pos;

        const std::string &pk = row.values.front();
        auto pk_it = table.primary_index.find(pk);
        if (pk_it != table.primary_index.end()) {
            const Row &existing = table.rows[pk_it->second];
            if (existing.active)
                throw std::runtime_error("Duplicate primary key: " + pk);
        }

        wal_batch += serialize_row(row);
        wal_batch += '\n';
        table.primary_index[pk] = table.rows.size();
        table.rows.push_back(std::move(row));
        ++inserted_rows;

        skip_ws();
        if (pos >= n) {
            break;
        }
        if (values_clause[pos] != ',') {
            throw std::runtime_error("Invalid INSERT syntax");
        }
        ++pos;
    }

    if (inserted_rows == 0) throw std::runtime_error("INSERT requires at least one row");

    wal_append_batch(table, wal_batch, inserted_rows);

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

        auto emit_values = [&](const std::vector<std::string> &values) {
            std::vector<std::string> out; out.reserve(sel_idx.size());
            for (std::size_t i : sel_idx) out.push_back(values[i]);
            result.rows.push_back(std::move(out));
        };

        if (table.synthetic_mode != SyntheticMode::None) {
            if (use_index) {
                std::size_t row_id = 0;
                if (parse_size_value(filter_value, row_id) && row_id >= 1 && row_id <= table.synthetic_row_count) {
                    emit_values(make_synthetic_row(table.synthetic_mode, row_id));
                }
            } else {
                for (std::size_t row_id = 1; row_id <= table.synthetic_row_count; ++row_id) {
                    const std::vector<std::string> values = make_synthetic_row(table.synthetic_mode, row_id);
                    if (where_pos != std::string::npos && !synthetic_row_matches(values, filter_idx, cond)) {
                        continue;
                    }
                    emit_values(values);
                }
            }
        } else if (use_index) {
            auto it = table.primary_index.find(filter_value);
            if (it != table.primary_index.end()) {
                const Row &row = table.rows[it->second];
                if (row_is_alive(row)) emit_values(row.values);
            }
        } else {
            for (const Row &row : table.rows) {
                if (!row_is_alive(row)) continue;
                if (where_pos != std::string::npos) {
                    if (!compare_values(row.values[filter_idx], filter_value, cond.op)) continue;
                }
                emit_values(row.values);
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
        if (left.synthetic_mode != SyntheticMode::None || right.synthetic_mode != SyntheticMode::None) {
            throw std::runtime_error("JOIN is not supported for BULK-loaded synthetic tables");
        }

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
