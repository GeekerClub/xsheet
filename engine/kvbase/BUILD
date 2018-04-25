cc_library(
    name = 'kvbase',
    srcs = [
        'kv_base.cc',
        'leveldb_base.cc'
    ],
    deps = [
        '//proto:status_code_proto',
        '//proto:tablet_proto',
        '//thirdparty/leveldb:leveldb',
        '//toft/system/threading:threading',
        '//toft/base/string:string'
    ]
)
