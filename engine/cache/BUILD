cc_library(
    name = 'cache',
    srcs = [
        'cache.cc',
        'hr_cache.cc'
    ],
    deps = [
        '//proto:status_code_proto',
        '//toft/base/string:string',
        '//toft/system/threading:threading',
        '//toft/system/timer:timer',
        '//thirdparty/glog:glog',
        '//toft/base:class_registry'
    ],
     link_all_symbols=True
)

cc_library(
    name = 'ldb_cache',
    srcs = [
        'ldb_cache.cc'
    ],
    deps = [
        ':cache',
        '//toft/system/atomic:atomic',
        '//toft/base/string:string',
        '//thirdparty/leveldb:leveldb'
    ]
)

cc_test(
    name = 'hr_cache_test',
    srcs = 'hr_cache_test.cc',
    deps = [
        ':cache',
        '//thirdparty/gtest:gtest'
    ]
)
