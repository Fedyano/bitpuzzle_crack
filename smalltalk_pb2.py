# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: smalltalk.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'smalltalk.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0fsmalltalk.proto\x12\tsmalltalk\"3\n\x0bTaskRequest\x12\x11\n\tdevice_id\x18\x01 \x01(\x05\x12\x11\n\tkey_count\x18\x02 \x01(\x03\";\n\x0cTaskResponse\x12\r\n\x05start\x18\x01 \x01(\t\x12\x0b\n\x03\x65nd\x18\x02 \x01(\t\x12\x0f\n\x07\x63ol_idx\x18\x03 \x01(\x05\"Q\n\x0f\x43ompleteRequest\x12\x11\n\tdevice_id\x18\x01 \x01(\x05\x12\r\n\x05start\x18\x02 \x01(\t\x12\x0b\n\x03\x65nd\x18\x03 \x01(\t\x12\x0f\n\x07\x63ol_idx\x18\x04 \x01(\x05\"\"\n\x10\x43ompleteResponse\x12\x0e\n\x06status\x18\x01 \x01(\t\"\x1e\n\x0bPingRequest\x12\x0f\n\x07message\x18\x01 \x01(\t\"\x1f\n\x0cPingResponse\x12\x0f\n\x07message\x18\x01 \x01(\t2\xd6\x01\n\x10SmallTalkService\x12<\n\x07GetTask\x12\x16.smalltalk.TaskRequest\x1a\x17.smalltalk.TaskResponse\"\x00\x12I\n\x0c\x43ompleteTask\x12\x1a.smalltalk.CompleteRequest\x1a\x1b.smalltalk.CompleteResponse\"\x00\x12\x39\n\x04Ping\x12\x16.smalltalk.PingRequest\x1a\x17.smalltalk.PingResponse\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'smalltalk_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_TASKREQUEST']._serialized_start=30
  _globals['_TASKREQUEST']._serialized_end=81
  _globals['_TASKRESPONSE']._serialized_start=83
  _globals['_TASKRESPONSE']._serialized_end=142
  _globals['_COMPLETEREQUEST']._serialized_start=144
  _globals['_COMPLETEREQUEST']._serialized_end=225
  _globals['_COMPLETERESPONSE']._serialized_start=227
  _globals['_COMPLETERESPONSE']._serialized_end=261
  _globals['_PINGREQUEST']._serialized_start=263
  _globals['_PINGREQUEST']._serialized_end=293
  _globals['_PINGRESPONSE']._serialized_start=295
  _globals['_PINGRESPONSE']._serialized_end=326
  _globals['_SMALLTALKSERVICE']._serialized_start=329
  _globals['_SMALLTALKSERVICE']._serialized_end=543
# @@protoc_insertion_point(module_scope)
