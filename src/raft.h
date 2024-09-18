#pragma once

#include "noncopyable.h"
#include "objfactory.h"
#include "thread_pool.h"
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace raft {
struct Log {
  int index = -1;
  int term = 0;
  bool is_server = false;
  std::string content;
};

enum class State {
  None = 0,
  Leader = 1,
  Candidate = 2,
  Follower = 3,
};

class server : public noncopyable {
  std::mutex m_mutex;
  std::shared_ptr<objfactory<server>> m_factory;

  int m_id = 0;
  bool m_is_stop = true;
  int m_heartbeat = 0;
  int m_vote_count = 0;

  // 需要持久化的数据
  State m_state = State::None;
  int m_term = 0;
  int m_votedfor = 0;
  std::vector<Log> m_log_vec;

  // 临时数据
  int m_commit_index = 0;
  int m_last_applied = 0;

  // 只属于leader的临时数据
  std::vector<int> m_next_index_vec;
  std::vector<int> m_match_index_vec;

public:
  server() = delete;
  server(int id, std::shared_ptr<objfactory<server>> factory);
  ~server() = default;

  int key() const { return m_id; }
  bool IsLeader() const { return m_state == State::Leader; }
  int Term() const { return m_term; }
  State GetState() const { return m_state; }
  bool IsStop() const { return m_is_stop; }
  const std::vector<Log> &LogVec() const { return m_log_vec; }
  const std::vector<Log> ApplyLogVec();

  int AddLog(const std::string &str);

  void Start();
  void Stop();
  void ReStart();

private:
  void Update(); // 定时器
  void Election();

  // 候选人请求投票，候选人信息
  struct VoteArgs {
    int term = 0;
    int candidate_id = 0;
    int last_log_index = -1;
    int last_log_term = 0;
  };
  // 回应投票
  struct VoteReply {
    int term = 0;
    bool vote_granted = false;
  };
  void RequestVote(const VoteArgs &args);
  void ReplyVote(const VoteReply &reply);

  // Leader追加条目，或心跳
  struct AppendEntriesArgs {
    int term = 0; // leader's term
    int leader_id = 0;
    int pre_log_index = 0;    // follower's replication index
    int pre_log_term = 0;     // follower's replication term
    int commit_index = 0;     // leader's latest commit index
    std::vector<Log> log_vec; // logs to be replicated
  };
  // 回应追加条目
  struct AppendEntriesReply {
    int id = 0;
    int term = 0;
    int log_count = 0; // the number of logs to be replicated
    bool success = false;
    int commit_index = 0;
  };
  void RequestAppendEntries(const AppendEntriesArgs &args);
  void ReplyAppendEntries(const AppendEntriesReply &reply);

  void ToLeader();
  void ToFollower(int term, int votedfor);

  // Print
private:
  std::map<long long, std::queue<std::string>> m_print_map;

public:
  void Print();
  void AddPrint(const std::string &str);
  void PrintAllLog();
  void PrintAllApplyLog();
};

} // namespace raft
