#include "raft.h"
#include "objfactory.h"
#include "thread_pool.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#define PRINT(...) PrintOutput(m_factory, m_id, __func__, ##__VA_ARGS__)

template <typename T> void osspack(std::ostream &os, T &&t) { os << t; }

template <typename T, typename... Args>
void osspack(std::ostream &os, T &&t, Args &&...args) {
  os << t;
  osspack(os, std::forward<Args>(args)...);
}

template <typename... Args>
void PrintOutput(std::shared_ptr<raft::objfactory<raft::server>> factory,
                 int id, const std::string &func, Args &&...args) {
  auto server = factory->Get(id, factory);
  std::ostringstream os;
  osspack(os, "(", id, " ", server->Term(), " ", (int)server->GetState(), ") ",
          func, "->", std::forward<Args>(args)...);
  factory->Get(0, factory)->AddPrint(os.str());
}

raft::server::server(int id, std::shared_ptr<objfactory<server>> factory) {
  assert(id >= 0);
  assert(factory);
  m_id = id;
  m_factory = factory;
}

const std::vector<raft::Log> raft::server::ApplyLogVec() {
  std::unique_lock<std::mutex> _(m_mutex);
  std::vector<Log> log_vec;
  for (int i = 0; i <= m_last_applied && i < (int)m_log_vec.size(); ++i) {
    if (!m_log_vec[i].is_server) {
      log_vec.emplace_back(m_log_vec[i]);
    }
  }
  return log_vec;
}

int raft::server::AddLog(const std::string &str) {
  std::unique_lock<std::mutex> _(m_mutex);
  if (m_is_stop || m_state != State::Leader) {
    return m_votedfor;
  }
  const auto &index = (int)m_log_vec.size();
  m_log_vec.emplace_back(Log{index, m_term, false, str});
  PRINT("index:", index, " term:", m_term, " content:", str);
  return 0;
}

void raft::server::Start() {
  std::unique_lock<std::mutex> _(m_mutex);
  m_is_stop = false;
  m_heartbeat = 0;
  m_vote_count = 0;

  m_state = State::Follower;
  m_term = 0;
  m_votedfor = 0;
  m_log_vec.clear();
  m_log_vec.emplace_back(Log{(int)m_log_vec.size(), 0, true, "Start"});

  m_commit_index = 0;
  m_last_applied = 0;

  m_next_index_vec.clear();
  m_match_index_vec.clear();

  // 启动定时器
  auto tmp = m_factory->Get(m_id, m_factory);
  thread_pool::get(0).submit([tmp] { tmp->Update(); });
}

void raft::server::Stop() {
  std::unique_lock<std::mutex> _(m_mutex);
  m_is_stop = true;
  PRINT("Stop");
}

void raft::server::ReStart() {
  std::unique_lock<std::mutex> _(m_mutex);
  m_is_stop = false;
  m_heartbeat = 0;
  m_vote_count = 0;

  // m_state=State::Follower;可省略

  m_next_index_vec.clear();
  m_match_index_vec.clear();
  PRINT("ReStart");
}

void raft::server::Update() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::unique_lock<std::mutex> _(m_mutex);
    if (m_is_stop) {
      continue;
    }
    switch (m_state) {
    case raft::State::Leader: {
      AppendEntriesArgs args;
      args.term = m_term;
      args.leader_id = m_id;
      args.commit_index = m_commit_index;
      for (const auto &id : m_factory->GetAllObjKey()) {
        if (id == m_id || id < 0 || id >= m_next_index_vec.size()) {
          continue;
        }

        const auto &next_index = m_next_index_vec[id];
        args.pre_log_index = next_index - 1;
        args.pre_log_term = args.pre_log_index >= 0 &&
                                    args.pre_log_index < (int)m_log_vec.size()
                                ? m_log_vec[args.pre_log_index].term
                                : 0;

        args.log_vec.clear();
        for (int i = next_index; i < (int)m_log_vec.size(); ++i) {
          args.log_vec.emplace_back(m_log_vec[i]);
        }

        auto tmp = m_factory->Get(id, m_factory);
        thread_pool::get(0).submit(
            [tmp, args] { tmp->RequestAppendEntries(args); });
      }
    } break;
    case raft::State::Candidate:
      break;
    case raft::State::Follower: {
      if (++m_heartbeat == 8) {
        m_heartbeat = 0;
        m_state = State::Candidate;
        auto tmp = m_factory->Get(m_id, m_factory);
        thread_pool::get(0).submit([tmp] { tmp->Election(); });
      }
    } break;
    default:
      return;
    }

    // 保存日志
    if (m_state == State::Leader && !m_match_index_vec.empty()) {
      auto match_vec = m_match_index_vec;
      std::sort(match_vec.begin(), match_vec.end());
      // 排序，mid_index提交了相当于超过半数提交了
      const auto &mid_index = match_vec[(int)match_vec.size() / 2];
      if (m_log_vec[mid_index].term == m_term) {
        // 能自动把之前的日志都提交
        m_commit_index = std::max(mid_index, m_commit_index);
      }
    }

    for (; m_last_applied <= m_commit_index; ++m_last_applied) {
      if (m_last_applied >= 0 && m_last_applied < (int)m_log_vec.size()) {
        const auto &log = m_log_vec[m_last_applied];
        if (!log.is_server)
          PRINT("apply_log[", m_last_applied, "]{index:", log.index,
                " term:", log.term, " content:", log.content, "}");

      } else {
        PRINT("apply_log[", m_last_applied, "] fail");
      }
    }
  }
}

void raft::server::Election() {
  while (true) {
    std::random_device rd;
    std::default_random_engine eng(rd());
    std::uniform_int_distribution<int> dist(150, 300);
    const auto &sleep = dist(eng);
    PRINT("sleep:", sleep);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep));

    std::unique_lock<std::mutex> _(m_mutex);
    if (m_is_stop || m_state != State::Candidate) {
      break;
    }

    // 任期+1，自投一票
    m_vote_count = 1;
    ++m_term;
    m_votedfor = m_id;
    PRINT("vote self");

    // 发起请求投票
    const VoteArgs &args{m_term, m_id, (int)m_log_vec.size() - 1,
                         m_log_vec.empty() ? 0 : m_log_vec.back().term};
    for (const auto &id : m_factory->GetAllObjKey()) {
      if (id == m_id)
        continue;

      auto tmp = m_factory->Get(id, m_factory);
      thread_pool::get(0).submit([tmp, args] { tmp->RequestVote(args); });
    }
  }
}

void raft::server::RequestVote(const VoteArgs &args) {
  std::unique_lock<std::mutex> _(m_mutex);
  if (m_is_stop)
    return;

  VoteReply reply{};

  // 候选人任期比我大，或者我没投票，或者投的是同一人
  if (args.term >= m_term || m_votedfor == 0 ||
      m_votedfor == args.candidate_id) {
    // 和候选人比较日志（任期和索引），决定是否成为跟随者
    const auto &last_log_term = m_log_vec.empty() ? 0 : m_log_vec.back().term;
    if (args.last_log_term > last_log_term ||
        (args.last_log_term == last_log_term &&
         args.last_log_index >= (int)m_log_vec.size() - 1)) {
      ToFollower(args.term, args.candidate_id);
      reply.vote_granted = true;
    }
  }

  if (!reply.vote_granted && m_state == State::Leader) {
    ToFollower(args.term, 0);
  }

  reply.term = m_term;

  PRINT(reply.vote_granted ? "vote: " : "not vote: ", args.candidate_id);

  // 投票返回
  auto tmp = m_factory->Get(args.candidate_id, m_factory);
  thread_pool::get(0).submit([tmp, reply] { tmp->ReplyVote(reply); });
}

void raft::server::ReplyVote(const VoteReply &reply) {
  std::unique_lock<std::mutex> _(m_mutex);
  // 收到投票，但已经不是候选人了
  if (m_is_stop || m_state != State::Candidate) {
    return;
  }

  // 同意投票，票数+1，超过半数，当选领导
  if (reply.vote_granted) {
    if (++m_vote_count >= ((int)m_factory->GetAllObjKey().size() + 1) / 2) {
      ToLeader();
    }
  } else if (reply.term > m_term) {
    // 不同意投票，回应的任期比我大，放弃候选人，成为跟随者
    ToFollower(reply.term, 0);
  }
}

void raft::server::RequestAppendEntries(const AppendEntriesArgs &args) {
  std::unique_lock<std::mutex> _(m_mutex);
  if (m_is_stop)
    return;

  AppendEntriesReply reply{};

  // 任期比领导大
  if (m_term > args.term) {
    reply.success = false;
    PRINT("my term bigger than leader");

  } else {
    // 重置心跳
    m_heartbeat = 0;

    // 任期与领导一致
    m_term = args.term;

    if (m_state != State::Follower) {
      ToFollower(args.term, args.leader_id);
    }

    if (args.log_vec.empty()) {
      if (m_commit_index < args.commit_index && args.pre_log_index >= 0 &&
          args.pre_log_index < (int)m_log_vec.size() &&
          m_log_vec[args.pre_log_index].term == args.pre_log_term) {
        m_commit_index = std::min(args.pre_log_index, args.commit_index);
      }

      // 心跳无返回
      return;
    }

    if (m_commit_index >= args.pre_log_index + (int)args.log_vec.size()) {
      reply.success = true;
    } else if (args.pre_log_index >= (int)m_log_vec.size() ||
               (args.pre_log_index >= 0 &&
                m_log_vec[args.pre_log_index].term != args.pre_log_term)

    ) {
      if (args.pre_log_index >= (int)m_log_vec.size()) {
        PRINT("not match: ", args.pre_log_index, " >= ", m_log_vec.size());
      } else {
        PRINT("not match: ", args.pre_log_index, " >=0 && ",
              m_log_vec[args.pre_log_index].term, " != ", args.pre_log_term);
      }

      reply.success = false;
    } else {
      PRINT("log push: ", m_commit_index, " ", args.commit_index, " ",
            args.pre_log_index, " ", args.pre_log_term, " ",
            args.log_vec.size());

      m_log_vec.resize(args.pre_log_index + 1);
      m_log_vec.insert(m_log_vec.end(), args.log_vec.begin(),
                       args.log_vec.end());

      // 更新我的提交记录
      if (m_commit_index < args.commit_index) {
        m_commit_index = std::min(args.commit_index,
                                  std::max(args.pre_log_index, m_commit_index));
      }

      reply.success = true;
    }
  }

  reply.id = m_id;
  reply.term = m_term;
  reply.log_count = (int)args.log_vec.size();
  reply.commit_index = m_commit_index;

  auto tmp = m_factory->Get(args.leader_id, m_factory);
  thread_pool::get(0).submit([tmp, reply] { tmp->ReplyAppendEntries(reply); });
}

void raft::server::ReplyAppendEntries(const AppendEntriesReply &reply) {
  std::unique_lock<std::mutex> _(m_mutex);
  if (m_is_stop || m_state != State::Leader) {
    PRINT("return stop:", m_is_stop, " state:", (int)m_state);
    return;
  }

  if (reply.id < 0 || reply.id >= (int)m_next_index_vec.size()) {
    PRINT("return id:", reply.id, " m_next_index_vec.size()",
          m_next_index_vec.size());
    return;
  }

  if (reply.success) {
    if (reply.id >= (int)m_match_index_vec.size()) {
      PRINT("return id:", reply.id, "relpy success, m_next_index_vec.size()",
            m_next_index_vec.size());
      return;
    }

    if (reply.log_count > 0) {
      PRINT("success:", reply.id, " ", m_match_index_vec[reply.id], " ",
            m_next_index_vec[reply.id], " count:", reply.log_count);
    }

    // 添加成功，更新跟随者的提交进度和同步进度
    m_match_index_vec[reply.id] =
        std::max(m_match_index_vec[reply.id],
                 m_next_index_vec[reply.id] + reply.log_count - 1);
    m_next_index_vec[reply.id] += reply.log_count;
  } else {
    if (reply.term <= m_term) {
      // 添加失败，更新跟随者的提交进度
      m_next_index_vec[reply.id] = reply.commit_index + 1;
      PRINT("fail:", reply.id, " ", m_next_index_vec[reply.id], " ",
            reply.commit_index);
    } else {
      // 任期比我大，不处理
    }
  }
}

void raft::server::ToLeader() {
  m_state = State::Leader;
  m_heartbeat = 0;
  m_vote_count = 0;
  m_votedfor = 0;

  {
    const auto &len = m_factory->GetAllObjKey().size();
    m_next_index_vec.clear();
    m_next_index_vec.resize(len, 0);
    m_match_index_vec.clear();
    m_match_index_vec.resize(len, 0);
  }

  m_log_vec.emplace_back(Log{(int)m_log_vec.size(), m_term, true,
                             "ToLeader:" + std::to_string(m_id)});
  PRINT("");
}

void raft::server::ToFollower(int term, int votedfor) {
  m_state = State::Follower;
  m_heartbeat = 0;
  m_vote_count = 0;
  m_term = term;
  m_votedfor = votedfor;
  PRINT("");
}

void raft::server::Print() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::unique_lock<std::mutex> _(m_mutex);
    if (m_print_map.empty()) {
      continue;
    }

    auto it = m_print_map.begin();
    while (!it->second.empty()) {
      PRINT("[%lld] %s\n", it->first, it->second.front().c_str());
      it->second.pop();
    }
    m_print_map.erase(it);
  }
}

void raft::server::AddPrint(const std::string &str) {
  std::unique_lock<std::mutex> _(m_mutex);
  m_print_map[std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::system_clock().now().time_since_epoch())
                  .count()]
      .push(str);
}

void raft::server::PrintAllLog() {
  std::unique_lock<std::mutex> _(m_mutex);
  for (const auto &log : m_log_vec) {
    PRINT("index:", log.index, " term:", log.term, " is_server:", log.is_server,
          " content:", log.content);
  }
}

void raft::server::PrintAllApplyLog() {
  const auto &log_vec = ApplyLogVec();
  std::unique_lock<std::mutex> _(m_mutex);
  for (const auto &log : log_vec) {
    PRINT("index:", log.index, " term:", log.term, " is_server:", log.is_server,
          " content:", log.content);
  }
}
