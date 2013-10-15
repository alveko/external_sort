#ifndef ASYNC_TASKS_HPP
#define ASYNC_TASKS_HPP

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <list>
#include <memory>

template <typename Task>
class AsyncTasks
{
  public:
    using TaskPtr = std::shared_ptr<Task>;

    AsyncTasks();

    TaskPtr NewTask() const { return TaskPtr(new Task()); }
    void AddTask(TaskPtr task);

    bool Empty() const { return All() == 0; }
    bool All() const { return Pending() + Running() + Ready(); }

    size_t Pending() const;
    size_t Running() const;
    size_t Ready() const;

    void StartAll();
    TaskPtr GetAny();

  private:
    void RunTask(TaskPtr task);

  private:
    TRACEX_NAME("AsyncTasks");

    mutable std::mutex mtx_;
    std::condition_variable cv_;

    std::list<TaskPtr>  tasks_pending_;
    std::atomic<size_t> tasks_running_;
    std::list<TaskPtr>  tasks_ready_;
};

template <typename Task>
AsyncTasks<Task>::AsyncTasks()
    : tasks_running_(0)
{
}

template <typename Task>
size_t AsyncTasks<Task>::Pending() const
{
    std::unique_lock<std::mutex> lck(mtx_);
    return tasks_pending_.size();
}

template <typename Task>
size_t AsyncTasks<Task>::Running() const
{
    return tasks_running_;
}

template <typename Task>
size_t AsyncTasks<Task>::Ready() const
{
    std::unique_lock<std::mutex> lck(mtx_);
    return tasks_ready_.size();
}

template <typename Task>
void AsyncTasks<Task>::AddTask(TaskPtr task)
{
    std::unique_lock<std::mutex> lck(mtx_);
    tasks_pending_.push_back(task);
    TRACEX(("task %p added (%02d/%02d/%02d)") % task
           % tasks_pending_.size() % tasks_running_ % tasks_ready_.size());
}

template <typename Task>
auto AsyncTasks<Task>::GetAny() -> TaskPtr
{
    TRACEX_METHOD();
    std::unique_lock<std::mutex> lck(mtx_);
    cv_.wait(lck, [ this ] () {
            return !(this->tasks_ready_.empty());
        });

    TaskPtr task = tasks_ready_.front();
    tasks_ready_.pop_front();
    TRACEX(("task %p collected (%02d/%02d/%02d)") % task
           % tasks_pending_.size() % tasks_running_ % tasks_ready_.size());
    return task;
}

template <typename Task>
void AsyncTasks<Task>::StartAll()
{
    TRACEX_METHOD();
    std::unique_lock<std::mutex> lck(mtx_);

    while (!tasks_pending_.empty()) {
        auto task = tasks_pending_.front();
        tasks_pending_.pop_front();

        std::thread thread(&AsyncTasks::RunTask, this, task);
        thread.detach();
        tasks_running_++;
        TRACEX(("task %p started (%02d/%02d/%02d)") % task
               % tasks_pending_.size() % tasks_running_ % tasks_ready_.size());
    }
    tasks_pending_.clear();
}

template <typename Task>
void AsyncTasks<Task>::RunTask(TaskPtr task)
{
    TRACEX_METHOD();
    task->Run();

    std::unique_lock<std::mutex> lck(mtx_);
    tasks_ready_.push_back(task);
    cv_.notify_one();
    tasks_running_--;
    TRACEX(("task %p ready (%02d/%02d/%02d)") % task
           % tasks_pending_.size() % tasks_running_ % tasks_ready_.size());
}

#endif
