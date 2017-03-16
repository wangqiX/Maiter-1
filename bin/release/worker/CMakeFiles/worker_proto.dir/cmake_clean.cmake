FILE(REMOVE_RECURSE
  "CMakeFiles/worker_proto"
  "worker.pb.cc"
  "worker.pb.h"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/worker_proto.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
