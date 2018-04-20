cc_library(
    name = 'key_coding',
    srcs = 'key_coding.cc',
    deps = [
        '//toft/base/string:string'
    ]
)

cc_library(
    name = 'string_utils',
    srcs = 'string_utils.cc',
    deps = [
        '//toft/base/string:string'
    ]
)

cc_library(
    name = 'raw_key',
    srcs = [
        'raw_key.cc',
        'raw_key_operator.cc'
    ],
    deps = [
        ':key_coding',
        ':string_utils',
        '//toft/base/string:string'
    ]
)

cc_test(
    name = 'key_coding_test',
    srcs = 'key_coding_test.cc',
    deps = [
        ':key_coding',
        '//toft/base/string:string',
        '//thirdparty/gtest:gtest'
    ]
)


cc_test(
    name = 'raw_key_test',
    srcs = 'raw_key_test.cc',
    deps = [
        ':raw_key',
        '//thirdparty/gtest:gtest'
    ]
)

cc_test(
    name = 'raw_key_operator_test',
    srcs = 'raw_key_operator_test.cc',
    deps = [
        ':raw_key',
        '//thirdparty/gtest:gtest'
    ]
)
