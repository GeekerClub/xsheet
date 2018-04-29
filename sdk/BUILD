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

cc_test(
    name = 'prop_tree_test',
    srcs = 'prop_tree_test.cc',
    deps = [
        ':schema_utils',
        '//thirdparty/gtest:gtest'
    ]
)
