#pragma once

// 在程序启动时（main 函数最开始）调用一次 InstallCrashHandler()，
// 之后如果程序发生未处理的异常（访问违例、断言失败触发的 abort 等），
// 会自动在指定目录下生成一个 .dmp 文件。
//
// 用法：
//   int main(int argc, char** argv) {
//       CrashDump::InstallCrashHandler("./crash_dumps");
//       ...
//   }
//
// 生成的 dmp 文件可以用 Visual Studio、WinDbg 打开，
// 只要保留对应版本的 .pdb（调试符号），就能看到完整的调用栈、
// 各线程状态、局部变量等，等价于事后能看到当时崩溃的"现场"。

#include <string>

namespace CrashDump
{

// dump_dir: dump 文件存放目录，如果不存在会自动创建。
//           传空字符串则使用当前工作目录。
// 返回 true 表示安装成功。
bool InstallCrashHandler(const std::string &dump_dir = "");

// 手动触发生成一份 dump（不需要真的崩溃），常用于：
//   - 程序检测到某种"不应该发生但也没直接崩"的异常状态，想留个现场
//   - 配合看门狗/信号处理，在收到某个信号时主动记录
// 返回生成的 dump 文件完整路径；失败返回空字符串。
std::string WriteDumpNow(const std::string &reason = "manual");

} // namespace CrashDump
