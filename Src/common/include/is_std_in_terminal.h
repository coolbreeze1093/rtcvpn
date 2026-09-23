
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
bool isStdinTerminal()
{
#ifdef _WIN32
    return _isatty(_fileno(stdin)) != 0;
#else
    return isatty(STDIN_FILENO) != 0;
#endif
}