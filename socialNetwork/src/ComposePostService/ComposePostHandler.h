#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_

#include <chrono>
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

std::chrono::seconds parse_duration(const std::string &str) {
    // Regular expression to extract the number (assuming it's seconds)
    std::regex regex("(\\d+)([smhd])");
    std::smatch match;
    
    if (regex_match(str, match, regex)) {
        int time_value = std::stoi(match[1].str());  // Extract the number
        char time_unit = match[2].str()[0];          // Extract the time unit (s, m, h, d)

        switch (time_unit) {
            case 's':
                return std::chrono::seconds(time_value);  // Return as seconds
            case 'm':
                return std::chrono::minutes(time_value);  // Return as minutes
            case 'h':
                return std::chrono::hours(time_value);    // Return as hours
            case 'd':
                return std::chrono::hours(time_value * 24);  // Return as days (converted to hours)
        }
    }

    return std::chrono::seconds(0);  // Default to 0 if invalid input
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
  LOG(info) << "Test 7";
  std::cout << "Test 7";
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
  LOG(info) << "Test 7";
  std::cout << "Test 7";
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
  LOG(info) << "Test 6";
  std::cout << "Test 6";
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
  LOG(info) << "Test 5";
  std::cout << "Test 5";
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
  LOG(info) << "Test 4";
  std::cout << "Test 4";
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
  LOG(info) << "Test 3";
  std::cout << "Test 3";
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

  LOG(info) << "Test 2";
  std::cout << "Test 2";

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

  LOG(info) << "Test 1";
  std::cout << "Test 1";

  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_post_server", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  LOG(info) << "compose_post opentracing completed";

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
      }
  }

  times_file.close();

  LOG(info) << "compose_post accessed JSON completed";

  auto text_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeTextHelper,
                 this, req_id, text, writer_text_map);
  auto creator_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeCreaterHelper,
                 this, req_id, user_id, username, writer_text_map);
  auto media_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeMediaHelper,
                 this, req_id, media_types, media_ids, writer_text_map);
  // auto unique_id_future = std::async(
  //     std::launch::async, &ComposePostHandler::_ComposeUniqueIdHelper, this,
  //     req_id, post_type, writer_text_map);

  LOG(info) << "future status completed";

  // Handle unique_id_future
  std::future_status unique_id_future_status;
  auto unique_id_future = std::async(std::launch::async, &ComposePostHandler::_ComposeUniqueIdHelper, this, req_id, post_type, writer_text_map);
  do {
      switch (unique_id_future_status = unique_id_future.wait_for(parse_duration(times["ComposePostService-unique_id_future"]["time"]))) {
          case std::future_status::deferred:
              break;
          case std::future_status::timeout:
              break;
          case std::future_status::ready:
              break;
      }
  } while (unique_id_future_status != std::future_status::ready);

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  // try
  // {
  auto start_time = std::chrono::system_clock::now(); 
  post.post_id = unique_id_future.get();
  auto end_time = std::chrono::system_clock::now();
  auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  LOG(info) << "ComposePost unique_id_future latency: " << latency << " ms";

  times["ComposePostService-unique_id_future"]["time"] = std::to_string(latency) + "ms";
  std::ofstream output_times_file("/mydata/adrita/socialnetwork-testbed/socialNetwork/src/timeout_values.txt");
  for (const auto& [key, value] : times) {
      output_times_file << key << " : " << value << "\n";
  }
  output_times_file.close();

  post.creator = creator_future.get();
  post.media = media_future.get();
  auto text_return = text_future.get();
  post.text = text_return.text;
  post.urls = text_return.urls;
  post.user_mentions = text_return.user_mentions;
  post.req_id = req_id;
  post.post_type = post_type;
  // }
  // catch (...)
  // {
  //   throw;
  // }

  std::vector<int64_t> user_mention_ids;
  for (auto &item : post.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }

  //In mixed workloed condition, need to make sure _UploadPostHelper execute
  //Before _UploadUserTimelineHelper and _UploadHomeTimelineHelper.
  //Change _UploadUserTimelineHelper and _UploadHomeTimelineHelper to deferred.
  //To let them start execute after post_future.get() return.
  auto post_future =
      std::async(std::launch::async, &ComposePostHandler::_UploadPostHelper,
                 this, req_id, post, writer_text_map);
  auto user_timeline_future = std::async(
      std::launch::deferred, &ComposePostHandler::_UploadUserTimelineHelper, this,
      req_id, post.post_id, user_id, timestamp, writer_text_map);
  auto home_timeline_future = std::async(
      std::launch::deferred, &ComposePostHandler::_UploadHomeTimelineHelper, this,
      req_id, post.post_id, user_id, timestamp, user_mention_ids,
      writer_text_map);

  // try
  // {
  post_future.get();
  user_timeline_future.get();
  home_timeline_future.get();
  // }
  // catch (...)
  // {
  //   throw;
  // }

  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_