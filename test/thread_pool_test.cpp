#include "../src/thread_pool.h"

#include <iostream>

int main(int argc, char *argv[]) {
  auto &tp = raft::thread_pool::get(4);
  std::vector<std::future<int>> results;
  for (int i = 0; i < 16; ++i) {
    results.emplace_back(tp.submit([i] {
      std::cout << "hello " << i << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(1));
      std::cout << "world " << i << std::endl;
      return i * i;
    }));
  }

  for (auto &&result : results) {
    std::cout << result.get() << " ";
    std::cout << std::endl;
  }
  return 0;
}
