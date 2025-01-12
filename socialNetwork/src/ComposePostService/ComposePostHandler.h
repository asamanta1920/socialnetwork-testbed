#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_

#include <chrono>
#include <iomanip>
#include <sstream>
#include <regex>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/MediaService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/TextService.h"
#include "../../gen-cpp/UniqueIdService.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"
#include <fstream>

using namespace std::chrono_literals;

namespace social_network {
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

std::string formatTimePoint(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

class ComposePostHandler : public ComposePostServiceIf {
 public:
  ComposePostHandler(ClientPool<ThriftClient<PostStorageServiceClient>> *,
                     ClientPool<ThriftClient<UserTimelineServiceClient>> *,
                     ClientPool<ThriftClient<UserServiceClient>> *,
                     ClientPool<ThriftClient<UniqueIdServiceClient>> *,
                     ClientPool<ThriftClient<MediaServiceClient>> *,
                     ClientPool<ThriftClient<TextServiceClient>> *,
                     ClientPool<ThriftClient<HomeTimelineServiceClient>> *);
  ~ComposePostHandler() override = default;

  void ComposePost(int64_t req_id, const std::string &username, int64_t user_id,
                   const std::string &text,
                   const std::vector<int64_t> &media_ids,
                   const std::vector<std::string> &media_types,
                   PostType::type post_type,
                   const std::map<std::string, std::string> &carrier) override;

 private:
  ClientPool<ThriftClient<PostStorageServiceClient>> *_post_storage_client_pool;
  ClientPool<ThriftClient<UserTimelineServiceClient>>
      *_user_timeline_client_pool;

  ClientPool<ThriftClient<UserServiceClient>> *_user_service_client_pool;
  ClientPool<ThriftClient<UniqueIdServiceClient>>
      *_unique_id_service_client_pool;
  ClientPool<ThriftClient<MediaServiceClient>> *_media_service_client_pool;
  ClientPool<ThriftClient<TextServiceClient>> *_text_service_client_pool;
  ClientPool<ThriftClient<HomeTimelineServiceClient>>
      *_home_timeline_client_pool;

  void _UploadUserTimelineHelper(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::map<std::string, std::string> &carrier);

  void _UploadPostHelper(int64_t req_id, const Post &post,
                         const std::map<std::string, std::string> &carrier);

  void _UploadHomeTimelineHelper(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::vector<int64_t> &user_mentions_id,
      const std::map<std::string, std::string> &carrier);

  Creator _ComposeCreaterHelper(
      int64_t req_id, int64_t user_id, const std::string &username,
      const std::map<std::string, std::string> &carrier);
  TextServiceReturn _ComposeTextHelper(
      int64_t req_id, const std::string &text,
      const std::map<std::string, std::string> &carrier);
  std::vector<Media> _ComposeMediaHelper(
      int64_t req_id, const std::vector<std::string> &media_types,
      const std::vector<int64_t> &media_ids,
      const std::map<std::string, std::string> &carrier);
  int64_t _ComposeUniqueIdHelper(
      int64_t req_id, PostType::type post_type,
      const std::map<std::string, std::string> &carrier);
};

ComposePostHandler::ComposePostHandler(
    ClientPool<social_network::ThriftClient<PostStorageServiceClient>>
        *post_storage_client_pool,
    ClientPool<social_network::ThriftClient<UserTimelineServiceClient>>
        *user_timeline_client_pool,
    ClientPool<ThriftClient<UserServiceClient>> *user_service_client_pool,
    ClientPool<ThriftClient<UniqueIdServiceClient>>
        *unique_id_service_client_pool,
    ClientPool<ThriftClient<MediaServiceClient>> *media_service_client_pool,
    ClientPool<ThriftClient<TextServiceClient>> *text_service_client_pool,
    ClientPool<ThriftClient<HomeTimelineServiceClient>>
        *home_timeline_client_pool) {
  _post_storage_client_pool = post_storage_client_pool;
  _user_timeline_client_pool = user_timeline_client_pool;
  _user_service_client_pool = user_service_client_pool;
  _unique_id_service_client_pool = unique_id_service_client_pool;
  _media_service_client_pool = media_service_client_pool;
  _text_service_client_pool = text_service_client_pool;
  _home_timeline_client_pool = home_timeline_client_pool;
}

Creator ComposePostHandler::_ComposeCreaterHelper(
    int64_t req_id, int64_t user_id, const std::string &username,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_creator_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto user_client_wrapper = _user_service_client_pool->Pop();
  if (!user_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    LOG(error) << se.message;
    span->Finish();
    throw se;
  }

  auto user_client = user_client_wrapper->GetClient();
  Creator _return_creator;
  try {
    user_client->ComposeCreatorWithUserId(_return_creator, req_id, user_id,
                                          username, writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to send compose-creator to user-service";
    _user_service_client_pool->Remove(user_client_wrapper);
    span->Finish();
    throw;
  }
  _user_service_client_pool->Keepalive(user_client_wrapper);
  span->Finish();
  return _return_creator;
}

TextServiceReturn ComposePostHandler::_ComposeTextHelper(
    int64_t req_id, const std::string &text,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_text_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto text_client_wrapper = _text_service_client_pool->Pop();
  if (!text_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to text-service";
    LOG(error) << se.message;
    ;
    span->Finish();
    throw se;
  }

  auto text_client = text_client_wrapper->GetClient();
  TextServiceReturn _return_text;
  try {
    text_client->ComposeText(_return_text, req_id, text, writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to send compose-text to text-service";
    _text_service_client_pool->Remove(text_client_wrapper);
    span->Finish();
    throw;
  }
  _text_service_client_pool->Keepalive(text_client_wrapper);
  span->Finish();
  return _return_text;
}

std::vector<Media> ComposePostHandler::_ComposeMediaHelper(
    int64_t req_id, const std::vector<std::string> &media_types,
    const std::vector<int64_t> &media_ids,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_media_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto media_client_wrapper = _media_service_client_pool->Pop();
  if (!media_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to media-service";
    LOG(error) << se.message;
    ;
    span->Finish();
    throw se;
  }

  auto media_client = media_client_wrapper->GetClient();
  std::vector<Media> _return_media;
  try {
    media_client->ComposeMedia(_return_media, req_id, media_types, media_ids,
                               writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to send compose-media to media-service";
    _media_service_client_pool->Remove(media_client_wrapper);
    span->Finish();
    throw;
  }
  _media_service_client_pool->Keepalive(media_client_wrapper);
  span->Finish();
  return _return_media;
}

int64_t ComposePostHandler::_ComposeUniqueIdHelper(
    int64_t req_id, const PostType::type post_type,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_unique_id_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto unique_id_client_wrapper = _unique_id_service_client_pool->Pop();
  if (!unique_id_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to unique_id-service";
    LOG(error) << se.message;
    span->Finish();
    throw se;
  }

  auto unique_id_client = unique_id_client_wrapper->GetClient();
  int64_t _return_unique_id;
  try {
    _return_unique_id =
        unique_id_client->ComposeUniqueId(req_id, post_type, writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to send compose-unique_id to unique_id-service";
    _unique_id_service_client_pool->Remove(unique_id_client_wrapper);
    span->Finish();
    throw;
  }
  _unique_id_service_client_pool->Keepalive(unique_id_client_wrapper);
  span->Finish();
  return _return_unique_id;
}

void ComposePostHandler::_UploadPostHelper(
    int64_t req_id, const Post &post,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "store_post_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto post_storage_client_wrapper = _post_storage_client_pool->Pop();
  if (!post_storage_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to post-storage-service";
    LOG(error) << se.message;
    ;
    throw se;
  }
  auto post_storage_client = post_storage_client_wrapper->GetClient();
  try {
    post_storage_client->StorePost(req_id, post, writer_text_map);
  } catch (...) {
    _post_storage_client_pool->Remove(post_storage_client_wrapper);
    LOG(error) << "Failed to store post to post-storage-service";
    throw;
  }
  _post_storage_client_pool->Keepalive(post_storage_client_wrapper);

  span->Finish();
}

void ComposePostHandler::_UploadUserTimelineHelper(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "write_user_timeline_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto user_timeline_client_wrapper = _user_timeline_client_pool->Pop();
  if (!user_timeline_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-timeline-service";
    LOG(error) << se.message;
    ;
    throw se;
  }
  auto user_timeline_client = user_timeline_client_wrapper->GetClient();
  try {
    user_timeline_client->WriteUserTimeline(req_id, post_id, user_id, timestamp,
                                            writer_text_map);
  } catch (...) {
    _user_timeline_client_pool->Remove(user_timeline_client_wrapper);
    throw;
  }
  _user_timeline_client_pool->Keepalive(user_timeline_client_wrapper);

  span->Finish();
}

void ComposePostHandler::_UploadHomeTimelineHelper(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto home_timeline_client_wrapper = _home_timeline_client_pool->Pop();
  if (!home_timeline_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to home-timeline-service";
    LOG(error) << se.message;
    ;
    throw se;
  }
  auto home_timeline_client = home_timeline_client_wrapper->GetClient();
  try {
    home_timeline_client->WriteHomeTimeline(req_id, post_id, user_id, timestamp,
                                            user_mentions_id, writer_text_map);
  } catch (...) {
    _home_timeline_client_pool->Remove(home_timeline_client_wrapper);
    LOG(error) << "Failed to write home timeline to home-timeline-service";
    throw;
  }
  _home_timeline_client_pool->Keepalive(home_timeline_client_wrapper);

  span->Finish();
}

void ComposePostHandler::ComposePost(
    const int64_t req_id, const std::string &username, int64_t user_id,
    const std::string &text, const std::vector<int64_t> &media_ids,
    const std::vector<std::string> &media_types, const PostType::type post_type,
    const std::map<std::string, std::string> &carrier) {
    TextMapReader reader(carrier);
    auto parent_span = opentracing::Tracer::Global()->Extract(reader);
    auto span = opentracing::Tracer::Global()->StartSpan(
        "compose_post_server", {opentracing::ChildOf(parent_span->get())});
    std::map<std::string, std::string> writer_text_map;
    TextMapWriter writer(writer_text_map);
    opentracing::Tracer::Global()->Inject(span->context(), writer);

    std::ifstream times_file("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
    std::map<std::string, std::string> times;
    std::string line;

    while (std::getline(times_file, line)) {
        if (line.empty() || line.find(':') == std::string::npos) {
            continue;
        }
        std::istringstream line_stream(line);
        std::string key, value;
        if (std::getline(line_stream, key, ':') && std::getline(line_stream, value)) {
            key.erase(key.find_last_not_of(" \t") + 1);
            key.erase(0, key.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            times[key] = value;
            LOG(info) << key << " " << value;
        }
    }

    times_file.close();

    std::ifstream waittimes_file("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/wait_times.txt");
    std::map<std::string, std::string> waittimes;
    std::string line2;

    while (std::getline(waittimes_file, line2)) {
        if (line2.empty() || line2.find(':') == std::string::npos) {
            continue;
        }
        std::istringstream line_stream2(line2);
        std::string key, value;
        if (std::getline(line_stream2, key, ':') && std::getline(line_stream2, value)) {
            key.erase(key.find_last_not_of(" \t") + 1);
            key.erase(0, key.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            waittimes[key] = value;
            LOG(info) << key << " " << value;
        }
    }

    waittimes_file.close();

    // Handle text_future
    std::future_status text_future_status;
    auto text_future =
        std::async(std::launch::async, &ComposePostHandler::_ComposeTextHelper,
                    this, req_id, text, writer_text_map);

    std::chrono::milliseconds timeout_ms_text_future(0);
    auto time_str1 = times["ComposePostService-text_future"];
    if (time_str1.empty() || !std::all_of(time_str1.begin(), time_str1.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str1;
    } else {
        std::chrono::milliseconds timeout_ms_text_future(std::stoi(time_str1));
    }

    std::chrono::milliseconds wait_time_text_future(0);
    auto time_str12 = waittimes["ComposePostService-text_future"];
    if (time_str12.empty() || !std::all_of(time_str12.begin(), time_str12.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str12;
    } else {
        std::chrono::milliseconds wait_time_text_future(std::stoi(time_str12));
    }

    do {
        text_future_status = text_future.wait_for(timeout_ms_text_future);

        switch (text_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred text_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for text_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready text_future";
                std::this_thread::sleep_for(wait_time_text_future);
                LOG(info) << "Forced extra time text_future";
                break;
        }
    } while (text_future_status != std::future_status::ready);

    // Handle creator_future
    std::future_status creator_future_status;
    auto creator_future =
        std::async(std::launch::async, &ComposePostHandler::_ComposeCreaterHelper,
                    this, req_id, user_id, username, writer_text_map);

    std::chrono::milliseconds timeout_ms_creator_future(0);
    auto time_str2 = times["ComposePostService-creator_future"];
    if (time_str2.empty() || !std::all_of(time_str2.begin(), time_str2.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str2;
    } else {
        std::chrono::milliseconds timeout_ms_creator_future(std::stoi(time_str2));
    }

    std::chrono::milliseconds wait_time_creator_future(0);
    auto time_str22 = times["ComposePostService-creator_future"];
    if (time_str22.empty() || !std::all_of(time_str22.begin(), time_str22.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str22;
    } else {
        std::chrono::milliseconds wait_time_creator_future(std::stoi(time_str22));
    }

    do {
        creator_future_status = creator_future.wait_for(timeout_ms_creator_future);

        switch (creator_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred creator_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for creator_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready creator_future";
                std::this_thread::sleep_for(wait_time_creator_future);
                LOG(info) << "Forced extra time creator_future";
                break;
        }
    } while (creator_future_status != std::future_status::ready);

    // Handle media_future
    std::future_status media_future_status;
    auto media_future =
        std::async(std::launch::async, &ComposePostHandler::_ComposeMediaHelper,
                    this, req_id, media_types, media_ids, writer_text_map);
    
    std::chrono::milliseconds timeout_ms_media_future(0);
    auto time_str3 = times["ComposePostService-media_future"];
    if (time_str3.empty() || !std::all_of(time_str3.begin(), time_str3.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str3;
    } else {
        std::chrono::milliseconds timeout_ms_media_future(std::stoi(time_str3));
    }

    std::chrono::milliseconds wait_time_media_future(0);
    auto time_str32 = times["ComposePostService-media_future"];
    if (time_str32.empty() || !std::all_of(time_str32.begin(), time_str32.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str32;
    } else {
        std::chrono::milliseconds wait_time_media_future(std::stoi(time_str32));
    }

    do {
        media_future_status = media_future.wait_for(timeout_ms_media_future);

        switch (media_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred media_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for media_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready media_future";
                std::this_thread::sleep_for(wait_time_media_future);
                LOG(info) << "Forced extra time media_future";
                break;
        }
    } while (media_future_status != std::future_status::ready);

    // Handle unique_id_future
    std::future_status unique_id_future_status;
    auto unique_id_future = std::async(std::launch::async, &ComposePostHandler::_ComposeUniqueIdHelper, this, req_id, post_type, writer_text_map);

    std::chrono::milliseconds timeout_ms_unique_id_future(0);
    auto time_str4 = times["ComposePostService-unique_id_future"];
    if (time_str4.empty() || !std::all_of(time_str4.begin(), time_str4.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str4;
    } else {
        std::chrono::milliseconds timeout_ms_unique_id_future(std::stoi(time_str4));
    }

    std::chrono::milliseconds wait_time_unique_id_future(0);
    auto time_str42 = times["ComposePostService-unique_id_future"];
    if (time_str42.empty() || !std::all_of(time_str42.begin(), time_str42.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str42;
    } else {
        std::chrono::milliseconds wait_time_unique_id_future(std::stoi(time_str42));
    }

    do {
        unique_id_future_status = unique_id_future.wait_for(timeout_ms_unique_id_future);

        switch (unique_id_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred unique_id_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for unique_id_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready unique_id_future";
                std::this_thread::sleep_for(wait_time_unique_id_future);
                LOG(info) << "Forced extra time unique_id_future";
                break;
        }
    } while (unique_id_future_status != std::future_status::ready);

    Post post;
    auto timestamp =
        duration_cast<milliseconds>(system_clock::now().time_since_epoch())
            .count();
    post.timestamp = timestamp;

    try {
        auto start_time_unique_id_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(start_time_unique_id_future);
        post.post_id = unique_id_future.get();
        auto end_time_unique_id_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(end_time_unique_id_future);
        auto latency_unique_id_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_unique_id_future - start_time_unique_id_future).count();
        LOG(info) << "ComposePost unique_id_future latency: " << latency_unique_id_future << " ms";
        times["ComposePostService-unique_id_future"] = std::to_string(latency_unique_id_future) + "ms";
        std::ofstream output_times_file_unique_id_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
        for (const auto& pair : times) {
            const auto& key = pair.first;
            const auto& value = pair.second;
            output_times_file_unique_id_future << key << ": " << value << std::endl;
        }
        output_times_file_unique_id_future.close();


        auto start_time_creator_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(start_time_creator_future);
        post.creator = creator_future.get();
        auto end_time_creator_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(end_time_creator_future);
        auto latency_creator_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_creator_future - start_time_creator_future).count();
        LOG(info) << "ComposePost creator_future latency: " << latency_creator_future << " ms";
        times["ComposePostService-creator_future"] = std::to_string(latency_creator_future) + "ms";
        std::ofstream output_times_file_creator_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
        for (const auto& pair : times) {
            const auto& key = pair.first;
            const auto& value = pair.second;
            output_times_file_creator_future << key << ": " << value << std::endl;
        }
        output_times_file_creator_future.close();

    
        auto start_time_media_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(start_time_media_future);
        post.media = media_future.get();
        auto end_time_media_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(end_time_media_future);
        auto latency_media_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_media_future - start_time_media_future).count();
        LOG(info) << "ComposePost media_future latency: " << latency_media_future << " ms";
        times["ComposePostService-media_future"] = std::to_string(latency_media_future) + "ms";
        std::ofstream output_times_file_media_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
        for (const auto& pair : times) {
            const auto& key = pair.first;
            const auto& value = pair.second;
            output_times_file_media_future << key << ": " << value << std::endl;
        }
        output_times_file_media_future.close();


        auto start_time_text_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(start_time_text_future);
        auto text_return = text_future.get();
        post.text = text_return.text;
        post.urls = text_return.urls;
        post.user_mentions = text_return.user_mentions;
        post.req_id = req_id;
        post.post_type = post_type;
        auto end_time_text_future = std::chrono::system_clock::now();
        LOG(info) << formatTimePoint(end_time_text_future);
        auto latency_text_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_text_future - start_time_text_future).count();
        LOG(info) << "ComposePost text_future latency: " << latency_text_future << " ms";
        times["ComposePostService-text_future"] = std::to_string(latency_text_future) + "ms";
        std::ofstream output_times_file_text_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
        for (const auto& pair : times) {
            const auto& key = pair.first;
            const auto& value = pair.second;
            output_times_file_text_future << key << ": " << value << std::endl;
        }
        output_times_file_text_future.close();

    }
    catch (const std::exception& e) {
        LOG(error) << "Error while composing post: " << e.what();
        throw;
    }

    std::vector<int64_t> user_mention_ids;
    for (auto &item : post.user_mentions) {
        user_mention_ids.emplace_back(item.user_id);
    }

    // Handle post_future
    std::future_status post_future_status;
    auto post_future = std::async(std::launch::async, &ComposePostHandler::_UploadPostHelper,
                                  this, req_id, post, writer_text_map);

    std::chrono::milliseconds timeout_ms_post_future(0);
    auto time_str5 = times["ComposePostService-post_future"];
    if (time_str5.empty() || !std::all_of(time_str5.begin(), time_str5.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str5;
    } else {
        std::chrono::milliseconds timeout_ms_post_future(std::stoi(time_str5));
    }

    std::chrono::milliseconds wait_time_post_future(0);
    auto time_str52 = times["ComposePostService-post_future"];
    if (time_str52.empty() || !std::all_of(time_str52.begin(), time_str52.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str52;
    } else {
        std::chrono::milliseconds wait_time_post_future(std::stoi(time_str52));
    }

    do {
        post_future_status = post_future.wait_for(timeout_ms_post_future);

        switch (post_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred post_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for post_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready post_future";
                std::this_thread::sleep_for(wait_time_post_future);
                LOG(info) << "Forced extra time post_future";
                break;
        }
    } while (post_future_status != std::future_status::ready);


    // Handle user_timeline_future
    std::future_status user_timeline_future_status;
    auto user_timeline_future = std::async(
        std::launch::deferred, &ComposePostHandler::_UploadUserTimelineHelper, this,
        req_id, post.post_id, user_id, timestamp, writer_text_map);

    std::chrono::milliseconds timeout_ms_user_timeline_future(0);
    auto time_str6 = times["ComposePostService-user_timeline_future"];
    if (time_str6.empty() || !std::all_of(time_str6.begin(), time_str6.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str6;
    } else {
        std::chrono::milliseconds timeout_ms_user_timeline_future(std::stoi(time_str6));
    }

    std::chrono::milliseconds wait_time_user_timeline_future(0);
    auto time_str62 = times["ComposePostService-user_timeline_future"];
    if (time_str6.empty() || !std::all_of(time_str62.begin(), time_str62.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str62;
    } else {
        std::chrono::milliseconds wait_time_user_timeline_future(std::stoi(time_str62));
    }

    do {
        user_timeline_future_status = user_timeline_future.wait_for(timeout_ms_user_timeline_future);

        switch (user_timeline_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred user_timeline_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for user_timeline_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready user_timeline_future";
                std::this_thread::sleep_for(wait_time_user_timeline_future);
                LOG(info) << "Forced extra time user_timeline_future";
                break;
        }
    } while (user_timeline_future_status != std::future_status::ready);


    // Handle home_timeline_future
    std::future_status home_timeline_future_status;
    auto home_timeline_future = std::async(
        std::launch::deferred, &ComposePostHandler::_UploadHomeTimelineHelper, this,
        req_id, post.post_id, user_id, timestamp, user_mention_ids,
        writer_text_map);

    std::chrono::milliseconds timeout_ms_home_timeline_future(0);
    auto time_str7 = times["ComposePostService-home_timeline_future"];
    if (time_str7.empty() || !std::all_of(time_str7.begin(), time_str7.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str7;
    } else {
        std::chrono::milliseconds timeout_ms_home_timeline_future(std::stoi(time_str7));
    }

    std::chrono::milliseconds wait_time_home_timeline_future(0);
    auto time_str72 = times["ComposePostService-home_timeline_future"];
    if (time_str72.empty() || !std::all_of(time_str72.begin(), time_str72.end(), ::isdigit)) {
        LOG(error) << "Invalid timeout value: " << time_str72;
    } else {
        std::chrono::milliseconds wait_time_home_timeline_future(std::stoi(time_str72));
    }

    do {
        home_timeline_future_status = home_timeline_future.wait_for(timeout_ms_home_timeline_future);

        switch (home_timeline_future_status) {
            case std::future_status::deferred:
                LOG(info) << "Deferred home_timeline_future";
                break;
            case std::future_status::timeout:
                LOG(info) << "Timeout waiting for home_timeline_future";
                break;
            case std::future_status::ready:
                LOG(info) << "Ready home_timeline_future";
                std::this_thread::sleep_for(wait_time_home_timeline_future);
                LOG(info) << "Forced extra time home_timeline_future";
                break;
        }
    } while (home_timeline_future_status != std::future_status::ready);

    auto start_time_post_future = std::chrono::system_clock::now();
    LOG(info) << formatTimePoint(start_time_post_future);
    post_future.get();
    auto end_time_post_future = std::chrono::system_clock::now();
    LOG(info) << formatTimePoint(end_time_post_future);
    auto latency_post_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_post_future - start_time_post_future).count();
    LOG(info) << "ComposePost post_future latency: " << latency_post_future << " ms";
    times["ComposePostService-post_future"] = std::to_string(latency_post_future) + "ms";
    std::ofstream output_times_file_post_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
    for (const auto& pair : times) {
        const auto& key = pair.first;
        const auto& value = pair.second;
        output_times_file_post_future << key << ": " << value << std::endl;
    }
    output_times_file_post_future.close();

    auto start_time_user_timeline_future = std::chrono::system_clock::now();
    LOG(info) << formatTimePoint(start_time_user_timeline_future);
    user_timeline_future.get();
    auto end_time_user_timeline_future = std::chrono::system_clock::now();
    LOG(info) << formatTimePoint(end_time_user_timeline_future);
    auto latency_user_timeline_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_user_timeline_future - start_time_user_timeline_future).count();
    LOG(info) << "ComposePost user_timeline_future latency: " << latency_user_timeline_future << " ms";
    times["ComposePostService-user_timeline_future"] = std::to_string(latency_user_timeline_future) + "ms";
    std::ofstream output_times_file_user_timeline_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
    for (const auto& pair : times) {
        const auto& key = pair.first;
        const auto& value = pair.second;
        output_times_file_user_timeline_future << key << ": " << value << std::endl;
    }
    output_times_file_user_timeline_future.close();

    auto start_time_home_timeline_future = std::chrono::system_clock::now();
    LOG(info) << formatTimePoint(start_time_home_timeline_future);
    home_timeline_future.get();
    auto end_time_home_timeline_future = std::chrono::system_clock::now();
    LOG(info) << formatTimePoint(end_time_home_timeline_future);
    auto latency_home_timeline_future = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_home_timeline_future - start_time_home_timeline_future).count();
    LOG(info) << "ComposePost home_timeline_future latency: " << latency_home_timeline_future << " ms";
    times["ComposePostService-home_timeline_future"] = std::to_string(latency_home_timeline_future) + "ms";
    std::ofstream output_times_file_home_timeline_future("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
    for (const auto& pair : times) {
        const auto& key = pair.first;
        const auto& value = pair.second;
        output_times_file_home_timeline_future << key << ": " << value << std::endl;
    }
    output_times_file_home_timeline_future.close();

    span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_