#include "crash_dump.h"
#include <string>
#if defined(_WIN32)
#include <windows.h>
#include <dbghelp.h>
#include <shlwapi.h>

#include <csignal>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <exception>


#endif

namespace CrashDump
{
#if defined(_WIN32)
namespace
{

std::string g_dump_dir;
std::once_flag g_install_once;

// 生成形如 "20260917-170243-1234.dmp" 的文件名
// （日期时间 + 进程 PID，避免多次崩溃互相覆盖）
std::string MakeDumpFileName(const std::string &reason)
{
    std::time_t t = std::time(nullptr);
    std::tm tm_buf;
    localtime_s(&tm_buf, &t);

    char time_part[32];
    std::strftime(time_part, sizeof(time_part), "%Y%m%d-%H%M%S", &tm_buf);

    char full[256];
    std::snprintf(full, sizeof(full), "%s\\crash_%s_%s_pid%lu.dmp",
                  g_dump_dir.c_str(), time_part, reason.c_str(),
                  static_cast<unsigned long>(GetCurrentProcessId()));
    return full;
}

// 真正写 dump 文件的核心函数。
// exception_pointers 为 nullptr 时（手动调用场景），
// 内部会构造一份"当前位置"的假异常信息，让 dump 里依然能看到调用栈。
bool WriteMiniDump(EXCEPTION_POINTERS *exception_pointers, const std::string &reason)
{
    std::string path = MakeDumpFileName(reason);

    HANDLE file = CreateFileA(path.c_str(), GENERIC_WRITE, 0, nullptr,
                               CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE)
    {
        return false;
    }

    MINIDUMP_EXCEPTION_INFORMATION mei{};
    MINIDUMP_EXCEPTION_INFORMATION *mei_ptr = nullptr;

    EXCEPTION_POINTERS local_ep{};
    CONTEXT local_ctx{};

    if (exception_pointers != nullptr)
    {
        mei.ThreadId = GetCurrentThreadId();
        mei.ExceptionPointers = exception_pointers;
        mei.ClientPointers = FALSE;
        mei_ptr = &mei;
    }
    else
    {
        // 手动触发场景：没有真实异常，构造一个指向当前位置的上下文，
        // 这样 dump 打开后依然能看到"当前调用栈"，而不是一片空白。
        RtlCaptureContext(&local_ctx);
        local_ep.ExceptionRecord = nullptr;
        local_ep.ContextRecord = &local_ctx;

        mei.ThreadId = GetCurrentThreadId();
        mei.ExceptionPointers = &local_ep;
        mei.ClientPointers = FALSE;
        mei_ptr = &mei;
    }

    // MiniDumpWithFullMemory 会把整个进程的内存都写进去，dump 会非常大，
    // 一般生产环境用下面这个级别就够用（带线程栈、局部变量、句柄信息），
    // 如果需要更详细的信息可以按需加别的 MiniDumpWithXxx 标志按位或起来。
    MINIDUMP_TYPE dump_type = static_cast<MINIDUMP_TYPE>(
        MiniDumpWithIndirectlyReferencedMemory |
        MiniDumpWithDataSegs |
        MiniDumpWithHandleData |
        MiniDumpWithThreadInfo);

    BOOL ok = MiniDumpWriteDump(
        GetCurrentProcess(), GetCurrentProcessId(), file,
        dump_type, mei_ptr, nullptr, nullptr);

    CloseHandle(file);
    return ok == TRUE;
}

LONG WINAPI UnhandledExceptionHandler(EXCEPTION_POINTERS *exception_pointers)
{
    WriteMiniDump(exception_pointers, "unhandled");
    // 返回 EXCEPTION_EXECUTE_HANDLER 会让系统按"已处理"结束进程，
    // 不再弹 Windows 的"程序已停止工作"对话框；
    // 如果你还想让调试器能接管（开发阶段调试用），可以改成
    // EXCEPTION_CONTINUE_SEARCH。
    return EXCEPTION_EXECUTE_HANDLER;
}

// 捕获 abort()（比如断言失败、std::terminate 触发的场景）
void SignalHandler(int sig)
{
    std::string reason = (sig == SIGABRT) ? "abort" : ("signal" + std::to_string(sig));
    WriteMiniDump(nullptr, reason);
    // 恢复默认处理，让进程照常终止（避免陷入信号处理死循环）
    std::signal(sig, SIG_DFL);
    std::raise(sig);
}

// std::terminate 的兜底（比如没被 catch 的 C++ 异常）
void TerminateHandler()
{
    WriteMiniDump(nullptr, "terminate");
    std::abort();
}

} // namespace
#endif

bool InstallCrashHandler(const std::string &dump_dir)
{
    bool installed_ok = true;
    #if defined(_WIN32)
    std::call_once(g_install_once, [&]()
    {
        g_dump_dir = dump_dir.empty() ? "." : dump_dir;

        // 去掉末尾多余的斜杠，保持拼路径时统一
        while (!g_dump_dir.empty() &&
               (g_dump_dir.back() == '\\' || g_dump_dir.back() == '/'))
        {
            g_dump_dir.pop_back();
        }

        SetUnhandledExceptionFilter(UnhandledExceptionHandler);

        std::signal(SIGABRT, SignalHandler);
        std::signal(SIGFPE, SignalHandler);
        std::signal(SIGILL, SignalHandler);

        std::set_terminate(TerminateHandler);
    });
    #endif

    return installed_ok;
}

std::string WriteDumpNow(const std::string &reason)
{
    #if defined(_WIN32)
    if (g_dump_dir.empty())
    {
        // 没调用过 InstallCrashHandler 也允许直接用，退化到当前目录
        g_dump_dir = ".";
    }
    std::string path = MakeDumpFileName(reason);
    return WriteMiniDump(nullptr, reason) ? path : std::string();
#else    
    return std::string();
#endif

}

} // namespace CrashDump
