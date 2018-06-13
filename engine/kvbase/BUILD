cc_library(
    name = 'kvbase',
    srcs = [
        'kv_base.cc',
        'leveldb_base.cc'
    ],
    deps = [
        '//engine/cache:cache',
        '//engine/cache:ldb_cache',
        '//engine:engine_flags',
        '//proto:status_code_proto',
        '//proto:tablet_proto',
        '//thirdparty/leveldb:leveldb',
        '//toft/system/threading:threading',
        '//toft/base/string:string',
        '//toft/base:class_registry'
    ],
    link_all_symbols=True
)
