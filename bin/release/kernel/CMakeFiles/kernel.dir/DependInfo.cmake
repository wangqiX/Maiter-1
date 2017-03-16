# The set of languages for which implicit dependencies are needed:
SET(CMAKE_DEPENDS_LANGUAGES
  "CXX"
  )
# The set of files for implicit dependencies of each language:
SET(CMAKE_DEPENDS_CHECK_CXX
  "/home/wangq/Maiter-1/src/kernel/global-table.cc" "/home/wangq/Maiter-1/bin/release/kernel/CMakeFiles/kernel.dir/global-table.cc.o"
  "/home/wangq/Maiter-1/src/kernel/kernel.cc" "/home/wangq/Maiter-1/bin/release/kernel/CMakeFiles/kernel.dir/kernel.cc.o"
  "/home/wangq/Maiter-1/src/kernel/local-table.cc" "/home/wangq/Maiter-1/bin/release/kernel/CMakeFiles/kernel.dir/local-table.cc.o"
  "/home/wangq/Maiter-1/src/kernel/table-registry.cc" "/home/wangq/Maiter-1/bin/release/kernel/CMakeFiles/kernel.dir/table-registry.cc.o"
  )
SET(CMAKE_CXX_COMPILER_ID "GNU")

# Preprocessor definitions for this target.
SET(CMAKE_TARGET_DEFINITIONS
  "PC_FROM_UCONTEXT=uc_mcontext.gregs[REG_RIP]"
  )

# Targets to which this target links.
SET(CMAKE_TARGET_LINKED_INFO_FILES
  )

# The include file search paths:
SET(CMAKE_C_TARGET_INCLUDE_PATH
  "kernel"
  "/home/wangq/Maiter-1/src/kernel"
  "/home/wangq/Maiter-1/src"
  "."
  "/usr/include/python2.7"
  "/usr/lib/openmpi/include"
  "/usr/lib/openmpi/include/openmpi"
  "/home/wangq/Maiter-1/src/external/google-flags"
  "/home/wangq/Maiter-1/src/external/google-logging"
  "/home/wangq/Maiter-1/src/external/webgraph"
  )
SET(CMAKE_CXX_TARGET_INCLUDE_PATH ${CMAKE_C_TARGET_INCLUDE_PATH})
SET(CMAKE_Fortran_TARGET_INCLUDE_PATH ${CMAKE_C_TARGET_INCLUDE_PATH})
SET(CMAKE_ASM_TARGET_INCLUDE_PATH ${CMAKE_C_TARGET_INCLUDE_PATH})
