# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: cache_segment.proto
# isort:skip_file

import sys

_b = sys.version_info[0] < 3 and (lambda x: x) or (lambda x: x.encode("latin1"))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor.FileDescriptor(
    name="cache_segment.proto",
    package="com.pennsieve",
    syntax="proto3",
    serialized_pb=_b(
        '\n\x13\x63\x61\x63he_segment.proto\x12\rcom.pennsieve">\n\x0c\x43\x61\x63heSegment\x12\x11\n\tchannelId\x18\x01 \x01(\t\x12\r\n\x05index\x18\x02 \x01(\x0c\x12\x0c\n\x04\x64\x61ta\x18\x03 \x01(\x0c\x62\x06proto3'
    ),
)


_CACHESEGMENT = _descriptor.Descriptor(
    name="CacheSegment",
    full_name="com.pennsieve.CacheSegment",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    fields=[
        _descriptor.FieldDescriptor(
            name="channelId",
            full_name="com.pennsieve.CacheSegment.channelId",
            index=0,
            number=1,
            type=9,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=_b("").decode("utf-8"),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            options=None,
        ),
        _descriptor.FieldDescriptor(
            name="index",
            full_name="com.pennsieve.CacheSegment.index",
            index=1,
            number=2,
            type=12,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=_b(""),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            options=None,
        ),
        _descriptor.FieldDescriptor(
            name="data",
            full_name="com.pennsieve.CacheSegment.data",
            index=2,
            number=3,
            type=12,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=_b(""),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            options=None,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=38,
    serialized_end=100,
)

DESCRIPTOR.message_types_by_name["CacheSegment"] = _CACHESEGMENT
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

CacheSegment = _reflection.GeneratedProtocolMessageType(
    "CacheSegment",
    (_message.Message,),
    dict(
        DESCRIPTOR=_CACHESEGMENT,
        __module__="cache_segment_pb2"
        # @@protoc_insertion_point(class_scope:com.pennsieve.CacheSegment)
    ),
)
_sym_db.RegisterMessage(CacheSegment)


# @@protoc_insertion_point(module_scope)
