#include "engine.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace flexql {

namespace {

std::string trim(const std::string &input) {
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return input.substr(start, end - start);
}

std::string without_semicolon(std::string input) {
    input = trim(input);
    if (!input.empty() && input.back() == ';') {
        input.pop_back();
    }
    return trim(input);
}

std::string upper(std::string input) {
    for (char &ch : input) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return input;
}

bool starts_with_keyword(const std::string &sql, const std::string &keyword) {
    if (sql.size() < keyword.size()) {
        return false;
    }
    return upper(sql.substr(0, keyword.size())) == keyword;
}

std::vector<std::string> split_csv(const std::string &input) {
    std::vector<std::string> parts;
    std::string current;
    bool in_quotes = false;
    for (std::size_t i = 0; i < input.size(); ++i) {
        const char ch = input[i];
        if (ch == '\'' && (i == 0 || input[i - 1] != '\\')) {
            in_quotes = !in_quotes;
            current.push_back(ch);
            continue;
        }
        if (ch == ',' && !in_quotes) {
            parts.push_back(trim(current));
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    if (!current.empty()) {
        parts.push_back(trim(current));
    }
    return parts;
}

std::vector<std::string> split_value_groups(const std::string &input) {
    std::vector<std::string> groups;
    std::string current;
    bool in_quotes = false;
    int depth = 0;

    for (std::size_t i = 0; i < input.size(); ++i) {
        const char ch = input[i];
        if (ch == '\'' && (i == 0 || input[i - 1] != '\\')) {
            in_quotes = !in_quotes;
        }

        if (!in_quotes && ch == '(') {
            if (depth == 0) {
                current.clear();
            } else {
                current.push_back(ch);
            }
            depth++;
            continue;
        }

        if (!in_quotes && ch == ')') {
            depth--;
            if (depth < 0) {
                throw std::runtime_error("Invalid INSERT value group");
            }
            if (depth == 0) {
                groups.push_back(trim(current));
                current.clear();
            } else {
                current.push_back(ch);
            }
            continue;
        }

        if (depth > 0) {
            current.push_back(ch);
        }
    }

    if (depth != 0) {
        throw std::runtime_error("Unbalanced parentheses in INSERT");
    }

    return groups;
}

std::string unquote(std::string value) {
    value = trim(value);
    if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
        value = value.substr(1, value.size() - 2);
    }
    std::string out;
    out.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '\\' && i + 1 < value.size()) {
            ++i;
        }
        out.push_back(value[i]);
    }
    return out;
}

bool try_parse_double(const std::string &text, double &value) {
    char *end = nullptr;
    value = std::strtod(text.c_str(), &end);
    return end != text.c_str() && end != nullptr && *end == '\0';
}

std::string normalize_identifier(std::string name) {
    return upper(trim(name));
}

std::string escape_field(const std::string &value) {
    std::string out;
    out.reserve(value.size());
    for (char ch : value) {
        if (ch == '\\' || ch == '|') {
            out.push_back('\\');
        }
        out.push_back(ch);
    }
    return out;
}

std::vector<std::string> split_escaped(const std::string &input, char delimiter) {
    std::vector<std::string> parts;
    std::string current;
    bool escaped = false;
    for (char ch : input) {
        if (escaped) {
            current.push_back(ch);
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == delimiter) {
            parts.push_back(current);
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    parts.push_back(current);
    return parts;
}

struct Condition {
    std::string column;
    std::string op;
    std::string value;
};

Condition parse_condition(const std::string &input) {
    static const std::vector<std::string> ops = {"<=", ">=", "!=", "=", "<", ">"};
    for (const std::string &op : ops) {
        const std::size_t pos = input.find(op);
        if (pos != std::string::npos) {
            return {trim(input.substr(0, pos)), op, trim(input.substr(pos + op.size()))};
        }
    }
    throw std::runtime_error("Unsupported WHERE/ON condition");
}

bool compare_values(const std::string &lhs, const std::string &rhs, const std::string &op) {
    double left_num = 0.0;
    double right_num = 0.0;
    const bool numeric = try_parse_double(lhs, left_num) && try_parse_double(rhs, right_num);

    if (numeric) {
        if (op == "=") {
            return left_num == right_num;
        }
        if (op == "!=") {
            return left_num != right_num;
        }
        if (op == "<") {
            return left_num < right_num;
        }
        if (op == "<=") {
            return left_num <= right_num;
        }
        if (op == ">") {
            return left_num > right_num;
        }
        if (op == ">=") {
            return left_num >= right_num;
        }
    }

    if (op == "=") {
        return lhs == rhs;
    }
    if (op == "!=") {
        return lhs != rhs;
    }
    if (op == "<") {
        return lhs < rhs;
    }
    if (op == "<=") {
        return lhs <= rhs;
    }
    if (op == ">") {
        return lhs > rhs;
    }
    if (op == ">=") {
        return lhs >= rhs;
    }
    return false;
}

std::size_t find_column_index(const std::vector<std::string> &columns, const std::string &name) {
    const std::string want = normalize_identifier(name);
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (normalize_identifier(columns[i]) == want) {
            return i;
        }
        const std::size_t dot = columns[i].find('.');
        if (dot != std::string::npos && normalize_identifier(columns[i].substr(dot + 1)) == want) {
            return i;
        }
    }
    throw std::runtime_error("Unknown column: " + name);
}

}  // namespace

Engine::Engine() {
    load_tables();
}

ExecuteResult Engine::execute(const std::string &raw_sql) {
    const std::string sql = without_semicolon(raw_sql);
    if (sql.empty()) {
        return {false, "Empty SQL statement", {}};
    }

    try {
        if (starts_with_keyword(sql, "CREATE TABLE")) {
            return create_table(sql);
        }
        if (starts_with_keyword(sql, "INSERT INTO")) {
            return insert_into(sql);
        }
        if (starts_with_keyword(sql, "SELECT")) {
            return select_from(sql);
        }
        return {false, "Unsupported SQL command", {}};
    } catch (const std::exception &ex) {
        return {false, ex.what(), {}};
    }
}

ExecuteResult Engine::create_table(const std::string &sql) {
    const std::size_t name_start = upper(sql).find("CREATE TABLE");
    const std::size_t open = sql.find('(', name_start + 12);
    const std::size_t close = sql.rfind(')');
    if (open == std::string::npos || close == std::string::npos || close <= open) {
        throw std::runtime_error("Invalid CREATE TABLE syntax");
    }

    const std::string raw_name = trim(sql.substr(name_start + 12, open - (name_start + 12)));
    const std::string table_name = normalize_identifier(raw_name);
    if (table_name.empty()) {
        throw std::runtime_error("Missing table name");
    }
    if (tables_.count(table_name) != 0U) {
        throw std::runtime_error("Table already exists: " + table_name);
    }

    Table table;
    table.name = table_name;
    const std::vector<std::string> defs = split_csv(sql.substr(open + 1, close - open - 1));
    if (defs.empty()) {
        throw std::runtime_error("CREATE TABLE needs at least one column");
    }

    for (const std::string &def : defs) {
        std::istringstream iss(def);
        std::string col_name;
        std::string type;
        iss >> col_name >> type;
        if (col_name.empty() || type.empty()) {
            throw std::runtime_error("Invalid column definition: " + def);
        }
        const std::string normalized_type = upper(type);
        ColumnType column_type;
        if (normalized_type == "DECIMAL" || normalized_type == "INT") {
            column_type = ColumnType::Decimal;
        } else if (normalized_type == "TEXT" || normalized_type.rfind("VARCHAR", 0) == 0) {
            column_type = ColumnType::Varchar;
        } else {
            throw std::runtime_error("Unsupported column type: " + type);
        }
        table.columns.push_back({normalize_identifier(col_name), column_type});
    }

    tables_[table_name] = std::move(table);
    persist_table(tables_[table_name]);
    return {true, {}, {}};
}

void Engine::trim_expired(Table &table) {
    const std::time_t now = std::time(nullptr);
    for (Row &row : table.rows) {
        if (row.active && row.expires_at <= now) {
            row.active = false;
        }
    }
}

ExecuteResult Engine::insert_into(const std::string &sql) {
    const std::string sql_upper = upper(sql);
    const std::size_t into_pos = sql_upper.find("INSERT INTO");
    const std::size_t values_pos = sql_upper.find("VALUES", into_pos + 11);
    if (values_pos == std::string::npos) {
        throw std::runtime_error("Invalid INSERT syntax");
    }

    const std::string table_name = normalize_identifier(sql.substr(into_pos + 11, values_pos - (into_pos + 11)));
    auto table_it = tables_.find(table_name);
    if (table_it == tables_.end()) {
        throw std::runtime_error("Unknown table: " + table_name);
    }
    Table &table = table_it->second;
    trim_expired(table);

    const std::string values_clause = trim(sql.substr(values_pos + 6));
    const std::vector<std::string> value_groups = split_value_groups(values_clause);
    if (value_groups.empty()) {
        throw std::runtime_error("INSERT requires at least one row");
    }

    for (const std::string &group : value_groups) {
        const std::vector<std::string> raw_values = split_csv(group);
        if (raw_values.size() != table.columns.size()) {
            throw std::runtime_error("INSERT column count does not match schema");
        }

        Row row;
        row.expires_at = std::time(nullptr) + kDefaultTtlSeconds;
        row.values.reserve(raw_values.size());

        for (std::size_t i = 0; i < raw_values.size(); ++i) {
            const Column &column = table.columns[i];
            std::string value = unquote(raw_values[i]);
            if (column.type == ColumnType::Decimal) {
                double parsed = 0.0;
                if (!try_parse_double(value, parsed)) {
                    throw std::runtime_error("Expected DECIMAL value for column " + column.name);
                }
                if (column.name == "EXPIRES_AT") {
                    row.expires_at = static_cast<std::time_t>(std::stoll(value));
                }
            }
            row.values.push_back(value);
        }

        const std::string &primary_key = row.values.front();
        if (table.primary_index.count(primary_key) != 0U) {
            const std::size_t idx = table.primary_index[primary_key];
            if (idx < table.rows.size() && table.rows[idx].active) {
                throw std::runtime_error("Duplicate primary key value: " + primary_key);
            }
        }

        table.rows.push_back(row);
        table.primary_index[primary_key] = table.rows.size() - 1;
    }

    persist_table(table);
    return {true, {}, {}};
}

ExecuteResult Engine::select_from(const std::string &sql) {
    const std::string sql_upper = upper(sql);
    const std::size_t from_pos = sql_upper.find(" FROM ");
    if (from_pos == std::string::npos) {
        throw std::runtime_error("Invalid SELECT syntax");
    }

    const std::string projection = trim(sql.substr(6, from_pos - 6));
    const std::string rest = trim(sql.substr(from_pos + 6));
    const std::string rest_upper = upper(rest);

    const std::size_t join_pos = rest_upper.find(" INNER JOIN ");
    const std::size_t where_pos = rest_upper.find(" WHERE ");

    QueryResult result;

    if (join_pos == std::string::npos) {
        const std::string table_name = normalize_identifier(rest.substr(0, where_pos == std::string::npos ? rest.size() : where_pos));
        auto table_it = tables_.find(table_name);
        if (table_it == tables_.end()) {
            throw std::runtime_error("Unknown table: " + table_name);
        }
        Table &table = table_it->second;
        trim_expired(table);

        std::vector<std::string> all_columns;
        all_columns.reserve(table.columns.size());
        for (const Column &column : table.columns) {
            all_columns.push_back(column.name);
        }

        std::vector<std::size_t> selected_indices;
        if (projection == "*") {
            result.columns = all_columns;
            for (std::size_t i = 0; i < all_columns.size(); ++i) {
                selected_indices.push_back(i);
            }
        } else {
            for (const std::string &part : split_csv(projection)) {
                const std::size_t idx = find_column_index(all_columns, part);
                selected_indices.push_back(idx);
                result.columns.push_back(all_columns[idx]);
            }
        }

        bool use_index = false;
        Condition condition;
        if (where_pos != std::string::npos) {
            condition = parse_condition(rest.substr(where_pos + 7));
            if (find_column_index(all_columns, condition.column) == 0 && condition.op == "=") {
                use_index = true;
            }
        }

        auto emit_row = [&](const Row &row) {
            std::vector<std::string> out;
            out.reserve(selected_indices.size());
            for (std::size_t idx : selected_indices) {
                out.push_back(row.values[idx]);
            }
            result.rows.push_back(std::move(out));
        };

        if (use_index) {
            const std::string key = unquote(condition.value);
            const auto it = table.primary_index.find(key);
            if (it != table.primary_index.end()) {
                const Row &row = table.rows[it->second];
                if (row.active) {
                    emit_row(row);
                }
            }
        } else {
            for (const Row &row : table.rows) {
                if (!row.active) {
                    continue;
                }
                bool passes = true;
                if (where_pos != std::string::npos) {
                    const std::size_t idx = find_column_index(all_columns, condition.column);
                    passes = compare_values(row.values[idx], unquote(condition.value), condition.op);
                }
                if (passes) {
                    emit_row(row);
                }
            }
        }
    } else {
        const std::string left_table_name = normalize_identifier(rest.substr(0, join_pos));
        const std::size_t on_pos = rest_upper.find(" ON ", join_pos + 12);
        if (on_pos == std::string::npos) {
            throw std::runtime_error("JOIN requires ON condition");
        }
        const std::string right_table_name = normalize_identifier(rest.substr(join_pos + 12, on_pos - (join_pos + 12)));
        const std::size_t local_where_pos = rest_upper.find(" WHERE ", on_pos + 4);
        const std::string join_condition_text = trim(rest.substr(on_pos + 4, local_where_pos == std::string::npos ? rest.size() - (on_pos + 4) : local_where_pos - (on_pos + 4)));
        const Condition join_condition = parse_condition(join_condition_text);

        auto left_it = tables_.find(left_table_name);
        auto right_it = tables_.find(right_table_name);
        if (left_it == tables_.end() || right_it == tables_.end()) {
            throw std::runtime_error("Unknown table in JOIN");
        }

        Table &left = left_it->second;
        Table &right = right_it->second;
        trim_expired(left);
        trim_expired(right);

        std::vector<std::string> all_columns;
        for (const Column &column : left.columns) {
            all_columns.push_back(left.name + "." + column.name);
        }
        for (const Column &column : right.columns) {
            all_columns.push_back(right.name + "." + column.name);
        }

        std::vector<std::size_t> selected_indices;
        if (projection == "*") {
            result.columns = all_columns;
            for (std::size_t i = 0; i < all_columns.size(); ++i) {
                selected_indices.push_back(i);
            }
        } else {
            for (const std::string &part : split_csv(projection)) {
                const std::size_t idx = find_column_index(all_columns, part);
                selected_indices.push_back(idx);
                result.columns.push_back(all_columns[idx]);
            }
        }

        const std::size_t left_join_idx = find_column_index(all_columns, join_condition.column);
        const std::size_t right_join_idx = find_column_index(all_columns, unquote(join_condition.value));

        bool has_where = local_where_pos != std::string::npos;
        Condition filter_condition;
        std::size_t filter_idx = 0;
        if (has_where) {
            filter_condition = parse_condition(rest.substr(local_where_pos + 7));
            filter_idx = find_column_index(all_columns, filter_condition.column);
        }

        for (const Row &left_row : left.rows) {
            if (!left_row.active) {
                continue;
            }
            for (const Row &right_row : right.rows) {
                if (!right_row.active) {
                    continue;
                }

                std::vector<std::string> joined = left_row.values;
                joined.insert(joined.end(), right_row.values.begin(), right_row.values.end());

                if (!compare_values(joined[left_join_idx], joined[right_join_idx], join_condition.op)) {
                    continue;
                }
                if (has_where && !compare_values(joined[filter_idx], unquote(filter_condition.value), filter_condition.op)) {
                    continue;
                }

                std::vector<std::string> projected;
                projected.reserve(selected_indices.size());
                for (std::size_t idx : selected_indices) {
                    projected.push_back(joined[idx]);
                }
                result.rows.push_back(std::move(projected));
            }
        }
    }

    remember_query(sql, result);
    return {true, {}, result};
}

void Engine::remember_query(const std::string &sql, const QueryResult &result) {
    auto existing = cache_lookup_.find(sql);
    if (existing != cache_lookup_.end()) {
        cache_order_.erase(existing->second);
        cache_lookup_.erase(existing);
    }
    cache_order_.push_front({sql, result});
    cache_lookup_[sql] = cache_order_.begin();
    if (cache_order_.size() > kCacheCapacity) {
        const auto last = std::prev(cache_order_.end());
        cache_lookup_.erase(last->key);
        cache_order_.pop_back();
    }
}

void Engine::load_tables() {
    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);

    for (const fs::directory_entry &entry : fs::directory_iterator(table_dir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".schema") {
            continue;
        }

        std::ifstream schema_file(entry.path());
        if (!schema_file) {
            continue;
        }

        Table table;
        table.name = normalize_identifier(entry.path().stem().string());

        std::string line;
        while (std::getline(schema_file, line)) {
            line = trim(line);
            if (line.empty()) {
                continue;
            }
            const std::vector<std::string> parts = split_escaped(line, '|');
            if (parts.size() != 2) {
                continue;
            }
            const std::string type = upper(parts[1]);
            table.columns.push_back({
                normalize_identifier(parts[0]),
                (type == "DECIMAL") ? ColumnType::Decimal : ColumnType::Varchar,
            });
        }

        const fs::path rows_path = table_dir / (table.name + ".rows");
        std::ifstream rows_file(rows_path);
        while (rows_file && std::getline(rows_file, line)) {
            if (trim(line).empty()) {
                continue;
            }
            const std::vector<std::string> parts = split_escaped(line, '|');
            if (parts.size() < 3 + table.columns.size()) {
                continue;
            }

            Row row;
            row.active = parts[0] == "1";
            row.expires_at = static_cast<std::time_t>(std::stoll(parts[1]));
            const std::size_t value_count = static_cast<std::size_t>(std::stoul(parts[2]));
            if (value_count != table.columns.size()) {
                continue;
            }
            row.values.reserve(value_count);
            for (std::size_t i = 0; i < value_count; ++i) {
                row.values.push_back(parts[3 + i]);
            }
            table.rows.push_back(std::move(row));
        }

        trim_expired(table);
        rebuild_primary_index(table);
        tables_[table.name] = std::move(table);
    }
}

void Engine::rebuild_primary_index(Table &table) {
    table.primary_index.clear();
    for (std::size_t i = 0; i < table.rows.size(); ++i) {
        if (table.rows[i].active && !table.rows[i].values.empty()) {
            table.primary_index[table.rows[i].values.front()] = i;
        }
    }
}

void Engine::persist_table(const Table &table) const {
    namespace fs = std::filesystem;
    const fs::path table_dir = fs::path(data_dir_) / "tables";
    fs::create_directories(table_dir);

    const fs::path schema_path = table_dir / (table.name + ".schema");
    const fs::path rows_path = table_dir / (table.name + ".rows");
    const fs::path schema_tmp = table_dir / (table.name + ".schema.tmp");
    const fs::path rows_tmp = table_dir / (table.name + ".rows.tmp");

    {
        std::ofstream schema_file(schema_tmp, std::ios::trunc);
        for (const Column &column : table.columns) {
            schema_file << escape_field(column.name) << '|'
                        << (column.type == ColumnType::Decimal ? "DECIMAL" : "VARCHAR") << '\n';
        }
    }

    {
        std::ofstream rows_file(rows_tmp, std::ios::trunc);
        for (const Row &row : table.rows) {
            rows_file << (row.active ? "1" : "0") << '|'
                      << static_cast<long long>(row.expires_at) << '|'
                      << row.values.size();
            for (const std::string &value : row.values) {
                rows_file << '|' << escape_field(value);
            }
            rows_file << '\n';
        }
    }

    fs::rename(schema_tmp, schema_path);
    fs::rename(rows_tmp, rows_path);
}

}  // namespace flexql
