proto_library(
    name = 'tablet_schema_proto',
    srcs = 'tablet_schema.proto'
)

cc_library(
    name = 'engine_flags',
    srcs = 'engine_flags.cc',
    deps = [
        '//thirdparty/gflags:gflags'
    ]
)

cc_library(
    name = 'tablet_utils',
    srcs = 'tablet_utils.cc',
    deps = [
        ':tablet_schema_proto',
        '//engine/codec:raw_key'
    ]
)

cc_library(
    name = 'tablet_writer',
    srcs = [
        'tablet_writer.cc'
    ],
    deps = [
        ':engine_flags',
        ':tablet_schema_proto',
        ':tablet_utils',
        '//engine/codec:raw_key',
        '//engine/kvbase:kvbase',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog',
        '//toft/system/threading:threading',
        '//toft/system/time:time'
    ]

)

cc_library(
    name = 'tablet_scanner',
    srcs = [
        'drop_checker.cc',
        'tablet_scanner.cc'
    ],
    deps = [
        ':tablet_schema_proto',
        ':tablet_utils',
        '//proto:tablet_proto',
        '//engine/codec:raw_key',
        '//toft/base/string:string',
        '//toft/system/time:time',
        '//thirdparty/glog:glog'
    ]
)

cc_library(
    name = 'tablet',
    srcs = [
        'tablet.cc'
    ],
    deps = [
        ':tablet_schema_proto',
        ':tablet_writer',
        ':tablet_scanner',
        '//engine/kvbase:kvbase',
        '//toft/storage/path:path',
        '//thirdparty/glog:glog'
    ]
)

cc_test(
    name = 'tablet_writer_test',
    srcs = 'tablet_writer_test.cc',
    deps = [
        ':tablet_writer',
        '//thirdparty/gflags:gflags',
        '//thirdparty/gtest:gtest'
    ]
)

cc_test(
    name = 'tablet_test',
    srcs = 'tablet_test.cc',
    deps = [
        ':tablet',
        '//thirdparty/gtest:gtest'
    ]
)
