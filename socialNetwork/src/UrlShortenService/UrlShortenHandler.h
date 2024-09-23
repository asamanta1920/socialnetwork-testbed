#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_URLSHORTENSERVICE_URLSHORTENHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_URLSHORTENSERVICE_URLSHORTENHANDLER_H_

#include <random>
#include <chrono>
#include <future>
#include <regex>
#include <nlohmann/json.hpp>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>

#include "../../gen-cpp/UrlShortenService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../logger.h"
#include "../tracing.h"

#define HOSTNAME "http://short-url/"

namespace social_network {

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

class UrlShortenHandler : public UrlShortenServiceIf {
 public:
  UrlShortenHandler(memcached_pool_st *, mongoc_client_pool_t *, std::mutex *);
  ~UrlShortenHandler() override = default;

  void ComposeUrls(std::vector<Url> &, int64_t,
      const std::vector<std::string> &,
      const std::map<std::string, std::string> &) override;

  void GetExtendedUrls(std::vector<std::string> &, int64_t,
                       const std::vector<std::string> &,
                       const std::map<std::string, std::string> &) override ;

 private:
  memcached_pool_st *_memcached_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  static std::mt19937 _generator;
  std::uniform_int_distribution<int> _distribution;
  std::string _GenRandomStr(int length);
  std::mutex *_thread_lock;
};

std::mt19937 UrlShortenHandler::_generator = std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count() % 0xffffffff);

UrlShortenHandler::UrlShortenHandler(
    memcached_pool_st *memcached_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    std::mutex *thread_lock) {
  _memcached_client_pool = memcached_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _thread_lock = thread_lock;
  _distribution = std::uniform_int_distribution<int>(0, 61);
}

std::string UrlShortenHandler::_GenRandomStr(int length) {
  const char char_map[] = "abcdefghijklmnopqrstuvwxyzABCDEF"
                    "GHIJKLMNOPQRSTUVWXYZ0123456789";
  std::string return_str;
  _thread_lock->lock();
  for (int i = 0; i < length; ++i) {
    return_str.append(1, char_map[_distribution(_generator)]);
  }
  _thread_lock->unlock();
  return return_str;
}
void UrlShortenHandler::ComposeUrls(
    std::vector<Url> &_return,
    int64_t req_id,
    const std::vector<std::string> &urls,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_urls_server",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::vector<Url> target_urls;
  std::future<void> mongo_future;

  if (!urls.empty()) {
    for (auto &url : urls) {
      Url new_target_url;
      new_target_url.expanded_url = url;
      new_target_url.shortened_url = HOSTNAME +
          _GenRandomStr(10);
      target_urls.emplace_back(new_target_url);
    }

    std::ifstream times_file("../wait_times.json");
    nlohmann::json times;
    times_file >> times;

    // Handle mongo_future
    std::future_status mongo_future_status;
    mongo_future = std::async(
        std::launch::async, [&](){
          mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
              _mongodb_client_pool);
          if (!mongodb_client) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_MONGODB_ERROR;
            se.message = "Failed to pop a client from MongoDB pool";
            throw se;
          }
          auto collection = mongoc_client_get_collection(
              mongodb_client, "url-shorten", "url-shorten");
          if (!collection) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_MONGODB_ERROR;
            se.message = "Failed to create collection user from DB user";
            mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
            throw se;
          }

          auto mongo_span = opentracing::Tracer::Global()->StartSpan(
              "url_mongo_insert_client",
              { opentracing::ChildOf(&span->context()) });

          mongoc_bulk_operation_t *bulk;
          bson_t *doc;
          bson_error_t error;
          bson_t reply;
          bool ret;
          bulk = mongoc_collection_create_bulk_operation_with_opts(
              collection, nullptr);
          for (auto &url : target_urls) {
            doc = bson_new();
            BSON_APPEND_UTF8(doc, "shortened_url", url.shortened_url.c_str());
            BSON_APPEND_UTF8(doc, "expanded_url", url.expanded_url.c_str());
            mongoc_bulk_operation_insert (bulk, doc);
            bson_destroy(doc);
          }
          ret = mongoc_bulk_operation_execute (bulk, &reply, &error);
          if (!ret) {
            LOG(error) << "MongoDB error: "<< error.message;
            ServiceException se;
            se.errorCode = ErrorCode::SE_MONGODB_ERROR;
            se.message = "Failed to insert urls to MongoDB";
            bson_destroy (&reply);
            mongoc_bulk_operation_destroy(bulk);
            mongoc_collection_destroy(collection);
            mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
            throw se;
          }
          bson_destroy (&reply);
          mongoc_bulk_operation_destroy(bulk);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          mongo_span->Finish();
        });
    do {
        switch (mongo_future_status = mongo_future.wait_for(parse_duration(times["UrlShortenService-mongo_future"]["time"]))) {
            case std::future_status::deferred:
                break;
            case std::future_status::timeout:
                break;
            case std::future_status::ready:
                break;
        }
    } while (mongo_future_status != std::future_status::ready);
      

  }

  if (!urls.empty()) {
    try {
      mongo_future.get();
    } catch (...) {
      LOG(error) << "Failed to upload shortened urls from MongoDB";
      throw;
    }
  }

  _return = target_urls;
  span->Finish();

}

void UrlShortenHandler::GetExtendedUrls(
    std::vector<std::string> &_return,
    int64_t req_id,
    const std::vector<std::string> &shortened_id,
    const std::map<std::string, std::string> &carrier) {

  // TODO: Implement GetExtendedUrls
}

}



#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_URLSHORTENSERVICE_URLSHORTENHANDLER_H_
