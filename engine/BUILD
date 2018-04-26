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
    name = 'tablet_writer',
    srcs = [
        'tablet_writer.cc',
        'tablet_utils.cc'
    ],
    deps = [
        ':engine_flags',
        ':tablet_schema_proto',
        '//engine/codec:raw_key',
        '//engine/kvbase:kvbase',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog',
        '//toft/system/threading:threading',
        '//toft/system/time:time'
    ]

)

cc_test(
    name = 'tablet_writer_test',
    srcs = 'tablet_writer_test.cc',
    deps = [
        ':tablet_writer',
        '//thirdparty/gtest:gtest'
    ]
)
