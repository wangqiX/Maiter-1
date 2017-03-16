FILE(REMOVE_RECURSE
  "CMakeFiles/common_proto"
  "common.pb.cc"
  "common.pb.h"
)

# Per-language clean rules from dependency scanning.
FOREACH(lang)
  INCLUDE(CMakeFiles/common_proto.dir/cmake_clean_${lang}.cmake OPTIONAL)
ENDFOREACH(lang)
