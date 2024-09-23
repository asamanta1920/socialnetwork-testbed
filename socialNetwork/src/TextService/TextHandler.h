#ifndef SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H

#include <future>
#include <iostream>
#include <regex>
#include <string>
#include <nlohmann/json.hpp>

#include "../../gen-cpp/TextService.h"
#include "../../gen-cpp/UrlShortenService.h"
#include "../../gen-cpp/UserMentionService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

namespace social_network {

std::chrono::seconds parse_duration(const std::string &str) {
    // Regular expression to extract the number (assuming it's seconds)
    regex regex("(\\d+)([smhd])");
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

class TextHandler : public TextServiceIf {
 public:
  TextHandler(ClientPool<ThriftClient<UrlShortenServiceClient>> *,
              ClientPool<ThriftClient<UserMentionServiceClient>> *);
  ~TextHandler() override = default;

  void ComposeText(TextServiceReturn &_return, int64_t, const std::string &,
                   const std::map<std::string, std::string> &) override;

 private:
  ClientPool<ThriftClient<UrlShortenServiceClient>> *_url_client_pool;
  ClientPool<ThriftClient<UserMentionServiceClient>> *_user_mention_client_pool;
};

TextHandler::TextHandler(
    ClientPool<ThriftClient<UrlShortenServiceClient>> *url_client_pool,
    ClientPool<ThriftClient<UserMentionServiceClient>>
        *user_mention_client_pool) {
  _url_client_pool = url_client_pool;
  _user_mention_client_pool = user_mention_client_pool;
}

void TextHandler::ComposeText(
    TextServiceReturn &_return, int64_t req_id, const std::string &text,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_text_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::vector<std::string> mention_usernames;
  std::smatch m;
  std::regex e("@[a-zA-Z0-9-_]+");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }

  std::vector<std::string> urls;
  e = "(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)";
  s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }

  std::ifstream times_file("../wait_times.json");
  nlohmann::json times;
  times_file >> times;
  // Handle shortened_urls_future
  std::future_status shortened_urls_future_status;
  auto shortened_urls_future = std::async(std::launch::async, [&]() {
    auto url_span = opentracing::Tracer::Global()->StartSpan(
        "compose_urls_client", {opentracing::ChildOf(&span->context())});

    std::map<std::string, std::string> url_writer_text_map;
    TextMapWriter url_writer(url_writer_text_map);
    opentracing::Tracer::Global()->Inject(url_span->context(), url_writer);

    auto url_client_wrapper = _url_client_pool->Pop();
    if (!url_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to url-shorten-service";
      throw se;
    }
    std::vector<Url> _return_urls;
    auto url_client = url_client_wrapper->GetClient();
    try {
      url_client->ComposeUrls(_return_urls, req_id, urls, url_writer_text_map);
    } catch (...) {
      LOG(error) << "Failed to upload urls to url-shorten-service";
      _url_client_pool->Remove(url_client_wrapper);
      throw;
    }
    _url_client_pool->Keepalive(url_client_wrapper);
    return _return_urls;
  });
  do {
      switch (shortened_urls_future_status = shortened_urls_future.wait_for(parse_duration(times["TextService-shortened_urls_future"]["time"]))) {
          case std::future_status::deferred:
              break;
          case std::future_status::timeout:
              break;
          case std::future_status::ready:
              break;
      }
  } while (shortened_urls_future_status != std::future_status::ready);

  // Handle user_mention_future
  std::future_status user_mention_future_status;
  auto user_mention_future = std::async(std::launch::async, [&]() {
    auto user_mention_span = opentracing::Tracer::Global()->StartSpan(
        "compose_user_mentions_client",
        {opentracing::ChildOf(&span->context())});

    std::map<std::string, std::string> user_mention_writer_text_map;
    TextMapWriter user_mention_writer(user_mention_writer_text_map);
    opentracing::Tracer::Global()->Inject(user_mention_span->context(),
                                          user_mention_writer);

    auto user_mention_client_wrapper = _user_mention_client_pool->Pop();
    if (!user_mention_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to user-mention-service";
      throw se;
    }
    std::vector<UserMention> _return_user_mentions;
    auto user_mention_client = user_mention_client_wrapper->GetClient();
    try {
      user_mention_client->ComposeUserMentions(_return_user_mentions, req_id,
                                               mention_usernames,
                                               user_mention_writer_text_map);
    } catch (...) {
      LOG(error) << "Failed to upload user_mentions to user-mention-service";
      _user_mention_client_pool->Remove(user_mention_client_wrapper);
      throw;
    }

    _user_mention_client_pool->Keepalive(user_mention_client_wrapper);
    return _return_user_mentions;
  });
  do {
      switch (user_mention_future_status = user_mention_future.wait_for(parse_duration(times["TextService-user_mention_future"]["time"]))) {
          case std::future_status::deferred:
              break;
          case std::future_status::timeout:
              break;
          case std::future_status::ready:
              break;
      }
  } while (user_mention_future_status != std::future_status::ready);

  std::vector<Url> target_urls;
  try {
    target_urls = shortened_urls_future.get();
  } catch (...) {
    LOG(error) << "Failed to get shortened urls from url-shorten-service";
    throw;
  }

  std::vector<UserMention> user_mentions;
  try {
    user_mentions = user_mention_future.get();
  } catch (...) {
    LOG(error) << "Failed to upload user mentions to user-mention-service";
    throw;
  }

  std::string updated_text;
  if (!urls.empty()) {
    s = text;
    int idx = 0;
    while (std::regex_search(s, m, e)) {
      auto url = m.str();
      urls.emplace_back(url);
      updated_text += m.prefix().str() + target_urls[idx].shortened_url;
      s = m.suffix().str();
      idx++;
    }
  } else {
    updated_text = text;
  }

  _return.user_mentions = user_mentions;
  _return.text = updated_text;
  _return.urls = target_urls;
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H