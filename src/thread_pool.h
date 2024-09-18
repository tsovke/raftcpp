#pragma once

#include "noncopyable.h"

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace raft {
class thread_pool : public noncopyable {
  std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_stop;
  std::queue<std::function<void()>> m_queue;
  std::vector<std::thread> m_threads;

public:
  static thread_pool &get(int thread_num) {
    static thread_pool tp(thread_num);
    return tp;
  }

  template <typename F, typename... Args>
  auto submit(F &&f, Args &&...args)
      -> std::future<std::invoke_result_t<F, Args...>> {
    using RT = std::invoke_result_t<F, Args...>;
    auto task = std::make_shared<std::packaged_task<RT()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    std::future<RT> res = task->get_future();
    {
      std::unique_lock<std::mutex> _(m_mutex);
      if (m_stop)
        throw std::runtime_error("enqueue on stoped thread_pool");

      m_queue.emplace([task]() { (*task)(); });
    }
    m_cv.notify_one();
    return res;
  }

private:
  thread_pool() = delete;

  thread_pool(int thread_num) : m_stop(false) {
    while (thread_num-- > 0) {
      m_threads.emplace_back(std::thread([this] { this->worker(); }));
    }
  }

  ~thread_pool() {
    {
      std::unique_lock<std::mutex> _(m_mutex);
      m_stop = true;
    }
    m_cv.notify_all();
    for (auto &worker : m_threads) {
      worker.join();
    }
  }

  void worker() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> _(m_mutex);
        m_cv.wait(_, [this] { return this->m_stop || !this->m_queue.empty(); });
        if (m_stop && m_queue.empty())
          return;
        task = std::move(m_queue.front());
        m_queue.pop();
      }
      task();
    }
  }
};

} // namespace raft
