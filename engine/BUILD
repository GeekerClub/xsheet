proto_library(
    name = 'tablet_schema_proto',
    srcs = 'tablet_schema.proto'
)

cc_library(
    name = 'tablet_writer',
    srcs = [
        'tablet_writer.cc',
        'tablet_utils.cc'
    ],
    deps = [
        ':tablet_schema_proto',
        '//engine/kvbase:kvbase',
        '//engine/codec:raw_key',
        '//toft/system/threading:threading',
        '//toft/system/time:time',
        '//thirdparty/gflags:gflags',
        '//thirdparty/glog:glog'
    ]

)
