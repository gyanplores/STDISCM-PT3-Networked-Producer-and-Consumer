#pragma once
// Shared by the producer and the consumer: reads config.json and checks numbers.
//
// Where a setting comes from (first one found wins):
//   1. the command line            e.g.  server 4 10
//   2. config.json                 e.g.  "consumer": { "threads": 4 }
//   3. the program asks you to type it (only for p, c and q)

#include <filesystem>
#include <fstream>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "json.hpp"

namespace cfg {

// True only if `text` is a whole number between lo and hi (inclusive).
// std::stoi alone would crash on "abc" and silently accept "5abc".
inline bool ParseIntInRange(const std::string& text, int lo, int hi, int& out) {
    try {
        size_t used = 0;
        long value = std::stol(text, &used);
        if (used != text.size() || value < lo || value > hi) return false;
        out = static_cast<int>(value);
        return true;
    } catch (...) {
        return false;
    }
}

// Loads config.json. If `explicit_path` is empty it looks for ./config.json and
// then ../config.json (so the server can be started from inside server/).
// No file found means "no config": an empty object, and the defaults apply.
// A file that exists but is broken stops the program with a clear message.
inline nlohmann::json Load(const std::string& explicit_path) {
    std::vector<std::string> candidates;
    if (explicit_path.empty()) {
        candidates = {"config.json", "../config.json"};
    } else {
        candidates = {explicit_path};
    }

    for (const auto& path : candidates) {
        if (!std::filesystem::exists(path)) continue;
        std::ifstream file(path);
        try {
            // 4th argument = true: allow // comments inside config.json
            nlohmann::json j = nlohmann::json::parse(file, nullptr, true, true);
            if (!j.is_object()) throw std::runtime_error("the top level must be { ... }");
            std::cout << "Using config file: " << std::filesystem::absolute(path).string() << "\n";
            return j;
        } catch (const std::exception& e) {
            std::cerr << "error: cannot read config file '" << path << "': " << e.what() << "\n";
            std::exit(1);
        }
    }

    if (!explicit_path.empty()) {
        std::cerr << "error: config file '" << explicit_path << "' does not exist\n";
        std::exit(1);
    }
    return nlohmann::json::object();
}

// Returns the section ("producer" or "consumer"), or an empty object if missing.
inline nlohmann::json Section(const nlohmann::json& config, const char* name) {
    if (config.contains(name) && config[name].is_object()) return config[name];
    return nlohmann::json::object();
}

// A setting as text ("" if it is missing). Numbers and true/false are converted,
// so the caller can treat "threads": 4 and "threads": "4" the same way.
inline std::string GetText(const nlohmann::json& section, const char* key) {
    if (!section.contains(key)) return "";
    const auto& v = section[key];
    if (v.is_string()) return v.get<std::string>();
    if (v.is_number_integer()) return std::to_string(v.get<long long>());
    if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
    return "";
}

inline bool GetBool(const nlohmann::json& section, const char* key, bool fallback) {
    std::string t = GetText(section, key);
    if (t == "true") return true;
    if (t == "false") return false;
    return fallback;
}

}  // namespace cfg
