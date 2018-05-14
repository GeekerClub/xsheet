cc_library(
    name = 'schema_utils',
    srcs = [
        'schema_utils.cc',
        'prop_tree.cc'
    ],
    deps = [
        '//engine:engine_flags',
        '//engine:tablet_schema_proto',
        '//toft/base/string:string'
    ]
)

cc_library(
    name = 'table',
    srcs = [
        'sdk_flags.cc',
        'scan.cc',
        'scan_impl.cc',
#         'table_impl.cc',
        'read_impl.cc',
        'mutate_impl.cc'
    ],
    deps = [
        ':schema_utils',
        '//proto:tablet_proto',
        '//engine:tablet_writer',
        '//engine:tablet_scanner'
    ]
)

cc_test(
    name = 'prop_tree_test',
    srcs = 'prop_tree_test.cc',
    deps = [
        ':schema_utils',
        '//thirdparty/gtest:gtest'
    ]
)
