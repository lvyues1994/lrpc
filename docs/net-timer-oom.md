# net 定时器堆扩容失败的已知限制

2026-09-27 在 net `7dff859`、co2 `a265e57` 上，通过全局 `operator new` 一次性故障注入确认：
新建 epoll context 的定时器首次入堆扩容失败，会触发 `std::terminate`，RPC 层无法捕获并排空。
按本轮约定，保持 `/home/lvyues/code/net/net` 不变。

调用路径为 `heap_timer::suspend` → `reactor_backend::add_timer` → `timer_heap::push` →
`std::vector<timer_op *>::push_back`。`push` 是 `noexcept`，内部扩容抛出 `bad_alloc` 后进程以 SIGABRT 退出。
gdb 回溯确认了该路径。简单删除 `noexcept` 并不足够：入堆失败还涉及工作计数、堆下标及 pending 状态回滚。

最小复现已保留在 `tests/fault_tests.cpp`。构建 Debug 后，以下命令会有意触发上述进程终止：

```sh
./build/debug/tests/fault_tests --reproduce-net-timer-oom
```

复现先构造 context、timer 与等待 task，随后让第一次入堆分配失败；不依赖网络监听，也不把异常清理改成强制退出。
它没有注册为“应通过”的 CTest 用例，不能把常规测试全绿解释为此故障已修复。

常规 `fault` 测试分别覆盖 RPC 准入的全局分配失败、公开调用任务的同步启动失败，以及通过 `net::memory_resource`
定向注入的握手/读写链协程帧分配失败。定向注入不改变 net 的普通分配器，因而不覆盖冷定时器堆扩容 OOM，
也不覆盖内部 `run_async` 完成状态对象的普通 `new` 失败。
它仍验证调用只完成一次、帧和预算释放、context 排空，以及连接仍可用时的准入恢复。

后续若修复依赖，应让入堆失败通过 I/O 错误返回，保持队列、工作计数和停止回调一致，并增加同一 timer 失败后可再次使用的测试。
