add_library(bson STATIC
  mutable/mutable_bson
  mutable/mutable_bson_builder
  mutable/mutable_bson_heap
  mutable/mutable_bson_internal
  util/bson_extract
  ../util/safe_num
  bson_validate
  oid
  ../db/jsobj
  ../db/json
  )
add_dependencies(bson generate_error_codes generate_action_types)
target_link_libraries(bson LINK_PUBLIC
  base
  md5
  stringutils
  platform
  )

install(FILES bsondemo/bsondemo.cpp
  DESTINATION src/mongo/bson/bsondemo
  COMPONENT tokumx_client_sources
  )
