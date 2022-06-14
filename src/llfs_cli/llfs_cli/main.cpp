//######=###=##=#=#=#=#=#==#==#====#+==#+==============+==+==+==+=+==+=+=+=+=+=+=+
// LLFS Command-Line Interface.
//

#include <llfs_cli/arena_command.hpp>
#include <llfs_cli/cache_command.hpp>

#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>

#include <llfs/config.hpp>

#include <iostream>

int main(int argc, char** argv)
{
  CLI::App app{"Low-Level File System (LLFS) Command Line Utility"};

  llfs_cli::add_arena_command(&app);
  llfs_cli::add_cache_command(&app);

  CLI11_PARSE(app, argc, argv);

  return 0;
}
