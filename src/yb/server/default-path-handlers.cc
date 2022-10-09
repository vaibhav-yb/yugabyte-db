// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "yb/server/default-path-handlers.h"

#include <sys/stat.h>

#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <set>

#include <boost/algorithm/string.hpp>
#include "yb/util/string_case.h"

#ifdef TCMALLOC_ENABLED
#include <gperftools/malloc_extension.h>
#endif

#include "yb/fs/fs_manager.h"

#include "yb/gutil/map-util.h"
#include "yb/gutil/strings/human_readable.h"
#include "yb/gutil/strings/split.h"
#include "yb/gutil/strings/substitute.h"
#include "yb/rpc/secure_stream.h"
#include "yb/server/pprof-path-handlers.h"
#include "yb/server/server_base.h"
#include "yb/server/secure.h"
#include "yb/server/webserver.h"
#include "yb/util/flag_tags.h"
#include "yb/util/format.h"
#include "yb/util/histogram.pb.h"
#include "yb/util/logging.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/memory/memory.h"
#include "yb/util/metrics.h"
#include "yb/util/jsonwriter.h"
#include "yb/util/result.h"
#include "yb/util/status_log.h"
#include "yb/util/version_info.h"
#include "yb/util/version_info.pb.h"

DEFINE_uint64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");
DECLARE_int32(max_tables_metrics_breakdowns);
TAG_FLAG(web_log_bytes, advanced);
TAG_FLAG(web_log_bytes, runtime);

DECLARE_bool(TEST_mini_cluster_mode);

namespace yb {

using boost::replace_all;
using google::CommandlineFlagsIntoString;
using std::ifstream;
using std::string;
using std::endl;
using std::shared_ptr;
using strings::Substitute;

using namespace std::placeholders;

namespace {

// Html/Text formatting tags
struct Tags {
  string pre_tag, end_pre_tag, line_break, header, end_header, table, end_table, row, end_row,
      table_header, end_table_header, cell, end_cell;

  // If as_text is true, set the html tags to a corresponding raw text representation.
  explicit Tags(bool as_text) {
    if (as_text) {
      pre_tag = "";
      end_pre_tag = "\n";
      line_break = "\n";
      header = "";
      end_header = "\n";
      table = "";
      end_table = "\n";
      row = "";
      end_row = "\n";
      table_header = "";
      end_table_header = "";
      cell = "";
      end_cell = "|";
    } else {
      pre_tag = "<pre>";
      end_pre_tag = "</pre>";
      line_break = "<br/>";
      header = "<h2>";
      end_header = "</h2>";
      table = "<table class='table table-striped'>";
      end_table = "</table>";
      row = "<tr>";
      end_row = "</tr>";
      table_header = "<th>";
      end_table_header = "</th>";
      cell = "<td>";
      end_cell = "</td>";
    }
  }
};

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
static void LogsHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;
  bool as_text = (req.parsed_args.find("raw") != req.parsed_args.end());
  Tags tags(as_text);
  string logfile;
  GetFullLogFilename(google::INFO, &logfile);
  (*output) << tags.header <<"INFO logs" << tags.end_header << endl;
  (*output) << "Log path is: " << logfile << endl;

  struct stat file_stat;
  if (stat(logfile.c_str(), &file_stat) == 0) {
    size_t size = file_stat.st_size;
    size_t seekpos = size < FLAGS_web_log_bytes ? 0L : size - FLAGS_web_log_bytes;
    ifstream log(logfile.c_str(), std::ios::in);
    // Note if the file rolls between stat and seek, this could fail
    // (and we could wind up reading the whole file). But because the
    // file is likely to be small, this is unlikely to be an issue in
    // practice.
    log.seekg(seekpos);
    (*output) << tags.line_break <<"Showing last " << FLAGS_web_log_bytes
              << " bytes of log" << endl;
    (*output) << tags.line_break << tags.pre_tag << log.rdbuf() << tags.end_pre_tag;

  } else {
    (*output) << tags.line_break << "Couldn't open INFO log file: " << logfile;
  }
}

std::vector<google::CommandLineFlagInfo> GetAllFlags(const Webserver::WebRequest& req) {
  std::vector<google::CommandLineFlagInfo> flag_infos;
  google::GetAllFlags(&flag_infos);

  if (FLAGS_TEST_mini_cluster_mode) {
    const string* custom_varz_ptr = FindOrNull(req.parsed_args, "TEST_custom_varz");
    if (custom_varz_ptr != nullptr) {
      map<string, string> varz;
      SplitStringToMapUsing(*custom_varz_ptr, "\n", &varz);

      // Replace values for existing flags.
      for (auto& flag_info : flag_infos) {
        auto varz_it = varz.find(flag_info.name);
        if (varz_it != varz.end()) {
          if (flag_info.current_value != varz_it->second) {
            flag_info.current_value = varz_it->second;
            flag_info.is_default = false;
          }
          varz.erase(varz_it);
        }
      }

      // Add new flags.
      for (auto const& flag : varz) {
        google::CommandLineFlagInfo flag_info;
        flag_info.name = flag.first;
        flag_info.current_value = flag.second;
        flag_info.default_value = "";
        flag_info.is_default = false;
        flag_infos.push_back(flag_info);
      }
    }
  }

  return flag_infos;
}

YB_DEFINE_ENUM(FlagType, (kInvalid)(kNodeInfo)(kCustom)(kAuto)(kDefault));

struct FlagInfo {
  string name;
  string value;
  FlagType type;
};

void ConvertFlagsToJson(const vector<FlagInfo>& flag_infos, std::stringstream* output) {
  JsonWriter jw(output, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("flags");
  jw.StartArray();

  for (const auto& flag_info : flag_infos) {
    jw.StartObject();
    jw.String("name");
    jw.String(flag_info.name);
    jw.String("value");
    jw.String(flag_info.value);
    jw.String("type");
    // Remove the prefix 'k' from the type name
    jw.String(ToString(flag_info.type).substr(1));
    jw.EndObject();
  }

  jw.EndArray();
  jw.EndObject();
}

vector<FlagInfo> GetFlagInfos(const Webserver::WebRequest& req) {
  const std::set<string> node_info_flags{
      "log_filename",    "rpc_bind_addresses", "webserver_interface", "webserver_port",
      "placement_cloud", "placement_region",   "placement_zone"};

  const auto flags = GetAllFlags(req);

  vector<FlagInfo> flag_infos;
  flag_infos.reserve(flags.size());

  for (const auto& flag : flags) {
    std::unordered_set<FlagTag> flag_tags;
    GetFlagTags(flag.name, &flag_tags);

    FlagInfo flag_info;
    flag_info.name = flag.name;
    flag_info.type = FlagType::kDefault;

    if (PREDICT_FALSE(ContainsKey(flag_tags, FlagTag::kSensitive_info))) {
      flag_info.value = "****";
    } else {
      flag_info.value = flag.current_value;
    }

    if (node_info_flags.contains(flag.name)) {
      flag_info.type = FlagType::kNodeInfo;
    } else if (flag.current_value != flag.default_value) {
      flag_info.type = FlagType::kCustom;
    } else if (flag_tags.contains(FlagTag::kAuto)) {
      flag_info.type = FlagType::kAuto;
    }

    flag_infos.push_back(std::move(flag_info));
  }

  // Sort by type, name ascending
  std::sort(flag_infos.begin(), flag_infos.end(), [](const FlagInfo& lhs, const FlagInfo& rhs) {
    if (lhs.type == rhs.type) {
      return ToLowerCase(lhs.name) < ToLowerCase(rhs.name);
    }
    return to_underlying(lhs.type) < to_underlying(rhs.type);
  });

  return flag_infos;
}

// Registered to handle "/api/v1/varz", and prints out all command-line flags and their values in
// JSON format.
static void GetFlagsJsonHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  const auto flag_infos = GetFlagInfos(req);
  ConvertFlagsToJson(std::move(flag_infos), &resp->output);
}

// Registered to handle "/varz", and prints out all command-line flags and their values in tabular
// format. If "raw" argument was passed ("/varz?raw") then prints it in "--name=value" format.
static void FlagsHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  std::stringstream& output = resp->output;
  auto flag_infos = GetFlagInfos(req);
  if (req.parsed_args.find("raw") != req.parsed_args.end()) {
    for (const auto& flag_info : flag_infos) {
      output << "--" << flag_info.name << "=" << flag_info.value << endl;
    }
    return;
  }

  Tags tags(false /* as_text */);

  // List is sorted by type. Convert to HTML table for each type.
  FlagType previous_type = FlagType::kInvalid;
  bool first_table = true;
  for (auto& flag_info : flag_infos) {
    if (previous_type != flag_info.type) {
      if (!first_table) {
        output << tags.end_table;
      }
      first_table = false;

      previous_type = flag_info.type;

      string type_str = ToString(flag_info.type).substr(1);
      output << tags.header << type_str << " Flags" << tags.end_header;
      output << tags.table << tags.row << tags.table_header << "Name" << tags.end_table_header
             << tags.table_header << "Value" << tags.end_table_header << tags.end_row;
    }

    output << tags.row << tags.cell << flag_info.name << tags.end_cell;
    output << tags.cell << flag_info.value << tags.end_cell << tags.end_row;
  }

  if (!first_table) {
    output << tags.end_table;
  }
}

// Registered to handle "/status", and simply returns empty JSON.
static void StatusHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;
  (*output) << "{}";
}

// Registered to handle "/memz", and prints out memory allocation statistics.
static void MemUsageHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;
  bool as_text = (req.parsed_args.find("raw") != req.parsed_args.end());
  Tags tags(as_text);

  (*output) << tags.pre_tag;
#ifndef TCMALLOC_ENABLED
  (*output) << "Memory tracking is not available unless tcmalloc is enabled.";
#else
  auto tmp = TcMallocStats();
  // Replace new lines with <br> for html.
  replace_all(tmp, "\n", tags.line_break);
  (*output) << tmp << tags.end_pre_tag;
#endif
}

// Registered to handle "/mem-trackers", and prints out to handle memory tracker information.
static void MemTrackersHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;
  *output << "<h1>Memory usage by subsystem</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Id</th><th>Current Consumption</th>"
      "<th>Peak consumption</th><th>Limit</th></tr>\n";

  int max_depth = INT_MAX;
  string depth = FindWithDefault(req.parsed_args, "max_depth", "");
  if (depth != "") {
    max_depth = std::stoi(depth);
  }
  string full_path_arg = FindWithDefault(req.parsed_args, "show_full_path", "true");
  bool use_full_path = ParseLeadingBoolValue(full_path_arg.c_str(), true);

  std::vector<MemTrackerData> trackers;
  CollectMemTrackerData(MemTracker::GetRootTracker(), 0, &trackers);
  for (const auto& data : trackers) {
    // If the data.depth >= max_depth, skip the info.
    if (data.depth > max_depth) {
      continue;
    }
    const auto& tracker = data.tracker;
    const std::string limit_str =
        tracker->limit() == -1 ? "none" : HumanReadableNumBytes::ToString(tracker->limit());
    const std::string current_consumption_str =
        HumanReadableNumBytes::ToString(tracker->consumption());
    const std::string peak_consumption_str =
        HumanReadableNumBytes::ToString(tracker->peak_consumption());
    const std::string tracker_id = use_full_path ? tracker->ToString() : tracker->id();
    *output << Format("  <tr data-depth=\"$0\" class=\"level$0\">\n", data.depth);
    *output << "    <td>" << tracker_id << "</td>";
    // UpdateConsumption returns true if consumption is taken from external source,
    // for instance tcmalloc stats. So we should show only it in this case.
    if (!data.consumption_excluded_from_ancestors || data.tracker->UpdateConsumption()) {
      *output << Format("<td>$0</td>", current_consumption_str);
    } else {
      auto full_consumption_str = HumanReadableNumBytes::ToString(
          tracker->consumption() + data.consumption_excluded_from_ancestors);
      *output << Format("<td>$0 ($1)</td>", current_consumption_str, full_consumption_str);
    }
    *output << Format("<td>$0</td><td>$1</td>\n", peak_consumption_str, limit_str);
    *output << "  </tr>\n";
  }

  *output << "</table>\n";
}

static Result<MetricLevel> MetricLevelFromName(const std::string& level) {
  if (level == "debug") {
    return MetricLevel::kDebug;
  } else if (level == "info") {
    return MetricLevel::kInfo;
  } else if (level == "warn") {
    return MetricLevel::kWarn;
  }
  return STATUS(NotSupported, Substitute("Unknown Metric Level $0", level));
}

template<class Value>
void SetParsedValue(Value* v, const Result<Value>& result) {
  if (result.ok()) {
    *v = *result;
  } else {
    LOG(WARNING) << "Can't parse option: " << result.status();
  }
}

bool ParseEntityOptions(const std::string& entity_prefix,
                        const Webserver::WebRequest& req,
                        MetricEntityOptions *metric_entity_options) {
  bool found = false;
  auto regex_p = FindOrNull(req.parsed_args, entity_prefix + "priority_regex");
  if (regex_p != nullptr) {
    found = true;
    metric_entity_options->priority_regex = *regex_p;
  }
  const string* metrics_p = FindOrNull(req.parsed_args, entity_prefix + "metrics");
  if (metrics_p != nullptr) {
    found = true;
    SplitStringUsing(*metrics_p, ",", &metric_entity_options->metrics);
  } else {
    metric_entity_options->metrics.push_back("*");
  }
  const string* exclude_metrics_p = FindOrNull(req.parsed_args, entity_prefix + "exclude_metrics");
  if (exclude_metrics_p != nullptr) {
    found = true;
    SplitStringUsing(*exclude_metrics_p, ",", &metric_entity_options->exclude_metrics);
  }
  return found;
}

static void ParseRequestOptions(const Webserver::WebRequest& req,
                                MeticEntitiesOptions *entities_options,
                                MetricPrometheusOptions *promethus_opts,
                                MetricJsonOptions *json_opts = nullptr,
                                JsonWriter::Mode *json_mode = nullptr) {
  if (entities_options) {
    MetricEntityOptions default_options;
    if (ParseEntityOptions("", req, &default_options)) {
      (*entities_options)[AggregationMetricLevel::kTable] = default_options;
    }
    MetricEntityOptions server_options;
    if (ParseEntityOptions("server_", req, &server_options)) {
      (*entities_options)[AggregationMetricLevel::kServer] = server_options;
    }
  }
  string arg;
  if (json_opts) {
    arg = FindWithDefault(req.parsed_args, "include_raw_histograms", "false");
    json_opts->include_raw_histograms = ParseLeadingBoolValue(arg.c_str(), false);

    arg = FindWithDefault(req.parsed_args, "include_schema", "false");
    json_opts->include_schema_info = ParseLeadingBoolValue(arg.c_str(), false);

    arg = FindWithDefault(req.parsed_args, "level", "debug");
    SetParsedValue(&json_opts->level, MetricLevelFromName(arg));
  }

  if (promethus_opts) {
    SetParsedValue(&promethus_opts->level,
                   MetricLevelFromName(FindWithDefault(req.parsed_args, "level", "debug")));
    promethus_opts->max_tables_metrics_breakdowns = std::stoi(FindWithDefault(req.parsed_args,
      "max_tables_metrics_breakdowns", std::to_string(FLAGS_max_tables_metrics_breakdowns)));
  }

  if (json_mode) {
    arg = FindWithDefault(req.parsed_args, "compact", "false");
    *json_mode =
        ParseLeadingBoolValue(arg.c_str(), false) ? JsonWriter::COMPACT : JsonWriter::PRETTY;
  }
}

static void WriteMetricsAsJson(const MetricRegistry* const metrics,
                               const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  MeticEntitiesOptions entities_opts;
  MetricJsonOptions opts;
  JsonWriter::Mode json_mode;
  ParseRequestOptions(req, &entities_opts, /* prometheus opts */ nullptr, &opts, &json_mode);
  if (entities_opts.empty()) {
    entities_opts[AggregationMetricLevel::kTable].metrics.push_back("*");
  }
  std::stringstream* output = &resp->output;
  JsonWriter writer(output, json_mode);

  WARN_NOT_OK(metrics->WriteAsJson(&writer,
                                   entities_opts[AggregationMetricLevel::kTable], opts),
              "Couldn't write JSON metrics over HTTP");
}

static void WriteMetricsForPrometheus(const MetricRegistry* const metrics,
                               const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  MetricPrometheusOptions opts;
  MeticEntitiesOptions entities_opts;
  ParseRequestOptions(req, &entities_opts, &opts);

  std::stringstream* output = &resp->output;

  std::set<std::string> prototypes;
  metrics->get_all_prototypes(prototypes);

  if (entities_opts.empty()) {
    if (prototypes.find("cdcsdk") != prototypes.end()) {
      entities_opts[AggregationMetricLevel::kStream].metrics.push_back("cdcsdk");
      prototypes.erase("cdcsdk");
    }

    entities_opts[AggregationMetricLevel::kTable].metrics.push_back("*");
    entities_opts[AggregationMetricLevel::kTable].exclude_metrics.push_back("cdcsdk");
  }

  for (const auto& entity_options : entities_opts) {
    PrometheusWriter writer(output, entity_options.first);
    WARN_NOT_OK(metrics->WriteForPrometheus(&writer, entity_options.second, opts),
                "Couldn't write text metrics for Prometheus");
  }
}

static void HandleGetVersionInfo(
    const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;

  VersionInfoPB version_info;
  VersionInfo::GetVersionInfoPB(&version_info);

  JsonWriter jw(output, JsonWriter::COMPACT);
  jw.StartObject();

  jw.String("build_id");
  jw.String(version_info.build_id());
  jw.String("build_type");
  jw.String(version_info.build_type());
  jw.String("build_number");
  jw.String(version_info.build_number());
  jw.String("build_timestamp");
  jw.String(version_info.build_timestamp());
  jw.String("build_username");
  jw.String(version_info.build_username());
  jw.String("version_number");
  jw.String(version_info.version_number());
  jw.String("build_hostname");
  jw.String(version_info.build_hostname());
  jw.String("git_revision");
  jw.String(version_info.git_hash());

  jw.EndObject();
}

} // anonymous namespace

void AddDefaultPathHandlers(Webserver* webserver) {
  webserver->RegisterPathHandler("/logs", "Logs", LogsHandler, true, false);
  webserver->RegisterPathHandler("/varz", "Flags", FlagsHandler, true, false);
  webserver->RegisterPathHandler("/status", "Status", StatusHandler, false, false);
  webserver->RegisterPathHandler("/memz", "Memory (total)", MemUsageHandler, true, false);
  webserver->RegisterPathHandler("/mem-trackers", "Memory (detail)",
                                 MemTrackersHandler, true, false);
  webserver->RegisterPathHandler("/api/v1/varz", "Flags", GetFlagsJsonHandler, false, false);
  webserver->RegisterPathHandler("/api/v1/version-info", "Build Version Info",
                                 HandleGetVersionInfo, false, false);

  AddPprofPathHandlers(webserver);
}

void RegisterMetricsJsonHandler(Webserver* webserver, const MetricRegistry* const metrics) {
  Webserver::PathHandlerCallback callback = std::bind(WriteMetricsAsJson, metrics, _1, _2);
  Webserver::PathHandlerCallback prometheus_callback = std::bind(
      WriteMetricsForPrometheus, metrics, _1, _2);
  bool not_styled = false;
  bool not_on_nav_bar = false;
  webserver->RegisterPathHandler("/metrics", "Metrics", callback, not_styled, not_on_nav_bar);

  // The old name -- this is preserved for compatibility with older releases of
  // monitoring software which expects the old name.
  webserver->RegisterPathHandler("/jsonmetricz", "Metrics", callback, not_styled, not_on_nav_bar);

  webserver->RegisterPathHandler(
      "/prometheus-metrics", "Metrics", prometheus_callback, not_styled, not_on_nav_bar);
}

// Registered to handle "/drives", and prints out paths usage
static void PathUsageHandler(FsManager* fsmanager,
                             const Webserver::WebRequest& req,
                             Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;
  *output << "<h1>Drives usage by subsystem</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Path</th><th>Used Space</th>"
      "<th>Total Space</th></tr>\n";

  Env* env = fsmanager->env();
  for (const auto& path : fsmanager->GetFsRootDirs()) {
    const auto stats = env->GetFilesystemStatsBytes(path);
    if (!stats.ok()) {
      LOG(WARNING) << stats.status();
      *output << Format("  <tr><td>$0</td><td colspan=\"2\">$1</td></tr>\n",
                        path, stats.status().message());
      continue;
    }
    const std::string used_space_str = HumanReadableNumBytes::ToString(stats->used_space);
    const std::string total_space_str = HumanReadableNumBytes::ToString(stats->total_space);
    *output << Format("  <tr><td>$0</td><td>$1</td><td>$2</td></tr>\n",
                      path, used_space_str, total_space_str);
  }
  *output << "</table>\n";
}

void RegisterPathUsageHandler(Webserver* webserver, FsManager* fsmanager) {
  Webserver::PathHandlerCallback callback = std::bind(PathUsageHandler, fsmanager, _1, _2);
  webserver->RegisterPathHandler("/drives", "Drives", callback, true, false);
}

// Registered to handle "/tls", and prints out certificate details
static void CertificateHandler(server::RpcServerBase* server,
                             const Webserver::WebRequest& req,
                             Webserver::WebResponse* resp) {
  std::stringstream *output = &resp->output;
  bool as_text = (req.parsed_args.find("raw") != req.parsed_args.end());
  Tags tags(as_text);
  (*output) << tags.header << "TLS Settings" << tags.end_header << endl;

  (*output) << tags.pre_tag;

  (*output) << "Node to node encryption enabled: "
      << (yb::server::IsNodeToNodeEncryptionEnabled() ? "true" : "false");

  (*output) << tags.line_break << "Client to server encryption enabled: "
      << (yb::server::IsClientToServerEncryptionEnabled() ? "true" : "false");

  (*output) << tags.line_break << "Allow insecure connections: "
      << (yb::rpc::AllowInsecureConnections() ? "on" : "off");

  (*output) << tags.line_break << "SSL Protocols: " << yb::rpc::GetSSLProtocols();

  (*output) << tags.line_break << "Cipher list: " << yb::rpc::GetCipherList();

  (*output) << tags.line_break << "Ciphersuites: " << yb::rpc::GetCipherSuites();

  (*output) << tags.end_pre_tag;

  auto details = server->GetCertificateDetails();

  if(!details.empty()) {
    (*output) << tags.header << "Certificate details" << tags.end_header << endl;

    (*output) << tags.pre_tag << details << tags.end_pre_tag << endl;
  }
}

void RegisterTlsHandler(Webserver* webserver, server::RpcServerBase* server) {
  Webserver::PathHandlerCallback callback = std::bind(CertificateHandler, server, _1, _2);
  webserver->RegisterPathHandler("/tls", "TLS", callback,
    true /*is_styled*/, false /*is_on_nav_bar*/);
}

} // namespace yb
