// Code generated by protoc-gen-go. DO NOT EDIT.
// source: google/devtools/cloudtrace/v2/tracing.proto

package cloudtrace // import "google.golang.org/genproto/googleapis/devtools/cloudtrace/v2"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import empty "github.com/golang/protobuf/ptypes/empty"
import _ "github.com/golang/protobuf/ptypes/timestamp"
import _ "google.golang.org/genproto/googleapis/api/annotations"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// The request message for the `BatchWriteSpans` method.
type BatchWriteSpansRequest struct {
	// Required. The name of the project where the spans belong. The format is
	// `projects/[PROJECT_ID]`.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// A list of new spans. The span names must not match existing
	// spans, or the results are undefined.
	Spans                []*Span  `protobuf:"bytes,2,rep,name=spans,proto3" json:"spans,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BatchWriteSpansRequest) Reset()         { *m = BatchWriteSpansRequest{} }
func (m *BatchWriteSpansRequest) String() string { return proto.CompactTextString(m) }
func (*BatchWriteSpansRequest) ProtoMessage()    {}
func (*BatchWriteSpansRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_tracing_18786c49399bd83d, []int{0}
}
func (m *BatchWriteSpansRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BatchWriteSpansRequest.Unmarshal(m, b)
}
func (m *BatchWriteSpansRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BatchWriteSpansRequest.Marshal(b, m, deterministic)
}
func (dst *BatchWriteSpansRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BatchWriteSpansRequest.Merge(dst, src)
}
func (m *BatchWriteSpansRequest) XXX_Size() int {
	return xxx_messageInfo_BatchWriteSpansRequest.Size(m)
}
func (m *BatchWriteSpansRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_BatchWriteSpansRequest.DiscardUnknown(m)
}

var xxx_messageInfo_BatchWriteSpansRequest proto.InternalMessageInfo

func (m *BatchWriteSpansRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *BatchWriteSpansRequest) GetSpans() []*Span {
	if m != nil {
		return m.Spans
	}
	return nil
}

func init() {
	proto.RegisterType((*BatchWriteSpansRequest)(nil), "google.devtools.cloudtrace.v2.BatchWriteSpansRequest")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// TraceServiceClient is the client API for TraceService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type TraceServiceClient interface {
	// Sends new spans to new or existing traces. You cannot update
	// existing spans.
	BatchWriteSpans(ctx context.Context, in *BatchWriteSpansRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	// Creates a new span.
	CreateSpan(ctx context.Context, in *Span, opts ...grpc.CallOption) (*Span, error)
}

type traceServiceClient struct {
	cc *grpc.ClientConn
}

func NewTraceServiceClient(cc *grpc.ClientConn) TraceServiceClient {
	return &traceServiceClient{cc}
}

func (c *traceServiceClient) BatchWriteSpans(ctx context.Context, in *BatchWriteSpansRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/google.devtools.cloudtrace.v2.TraceService/BatchWriteSpans", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *traceServiceClient) CreateSpan(ctx context.Context, in *Span, opts ...grpc.CallOption) (*Span, error) {
	out := new(Span)
	err := c.cc.Invoke(ctx, "/google.devtools.cloudtrace.v2.TraceService/CreateSpan", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TraceServiceServer is the server API for TraceService service.
type TraceServiceServer interface {
	// Sends new spans to new or existing traces. You cannot update
	// existing spans.
	BatchWriteSpans(context.Context, *BatchWriteSpansRequest) (*empty.Empty, error)
	// Creates a new span.
	CreateSpan(context.Context, *Span) (*Span, error)
}

func RegisterTraceServiceServer(s *grpc.Server, srv TraceServiceServer) {
	s.RegisterService(&_TraceService_serviceDesc, srv)
}

func _TraceService_BatchWriteSpans_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchWriteSpansRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TraceServiceServer).BatchWriteSpans(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/google.devtools.cloudtrace.v2.TraceService/BatchWriteSpans",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TraceServiceServer).BatchWriteSpans(ctx, req.(*BatchWriteSpansRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TraceService_CreateSpan_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Span)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TraceServiceServer).CreateSpan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/google.devtools.cloudtrace.v2.TraceService/CreateSpan",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TraceServiceServer).CreateSpan(ctx, req.(*Span))
	}
	return interceptor(ctx, in, info, handler)
}

var _TraceService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "google.devtools.cloudtrace.v2.TraceService",
	HandlerType: (*TraceServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "BatchWriteSpans",
			Handler:    _TraceService_BatchWriteSpans_Handler,
		},
		{
			MethodName: "CreateSpan",
			Handler:    _TraceService_CreateSpan_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "google/devtools/cloudtrace/v2/tracing.proto",
}

func init() {
	proto.RegisterFile("google/devtools/cloudtrace/v2/tracing.proto", fileDescriptor_tracing_18786c49399bd83d)
}

var fileDescriptor_tracing_18786c49399bd83d = []byte{
	// 404 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x52, 0xdd, 0x6a, 0xdb, 0x30,
	0x14, 0x46, 0xde, 0x0f, 0x4c, 0x1b, 0x0c, 0x04, 0x0b, 0xc1, 0xdb, 0x58, 0xe6, 0x0d, 0x96, 0x64,
	0x43, 0x02, 0x8f, 0x5d, 0x2c, 0x63, 0x37, 0x09, 0x23, 0xb7, 0x21, 0x19, 0x19, 0x8c, 0xdc, 0x28,
	0x8e, 0xa6, 0x69, 0xd8, 0x92, 0x67, 0x29, 0x86, 0x52, 0x7a, 0xd3, 0x9b, 0x3e, 0x40, 0xfb, 0x14,
	0xa5, 0xd0, 0xf7, 0xe8, 0x6d, 0x5f, 0xa1, 0x0f, 0x52, 0x24, 0xd9, 0x0d, 0x84, 0x34, 0xc9, 0x9d,
	0xce, 0x39, 0xdf, 0xf9, 0xce, 0xf7, 0x7d, 0x36, 0xfc, 0xc8, 0x95, 0xe2, 0x29, 0x23, 0x0b, 0x56,
	0x1a, 0xa5, 0x52, 0x4d, 0x92, 0x54, 0x2d, 0x17, 0xa6, 0xa0, 0x09, 0x23, 0x65, 0x4c, 0xec, 0x43,
	0x48, 0x8e, 0xf3, 0x42, 0x19, 0x85, 0x5e, 0x7b, 0x30, 0xae, 0xc1, 0x78, 0x05, 0xc6, 0x65, 0x1c,
	0xbe, 0xaa, 0xb8, 0x68, 0x2e, 0x08, 0x95, 0x52, 0x19, 0x6a, 0x84, 0x92, 0xda, 0x2f, 0x87, 0x9d,
	0xdd, 0x97, 0x58, 0x05, 0x7d, 0x59, 0x41, 0x5d, 0x35, 0x5f, 0xfe, 0x21, 0x2c, 0xcb, 0xcd, 0x41,
	0x35, 0x7c, 0xb3, 0x3e, 0x34, 0x22, 0x63, 0xda, 0xd0, 0x2c, 0xf7, 0x80, 0x88, 0xc3, 0x46, 0x9f,
	0x9a, 0xe4, 0xef, 0xaf, 0x42, 0x18, 0x36, 0xc9, 0xa9, 0xd4, 0x63, 0xf6, 0x7f, 0xc9, 0xb4, 0x41,
	0x08, 0x3e, 0x94, 0x34, 0x63, 0x4d, 0xd0, 0x02, 0xed, 0x27, 0x63, 0xf7, 0x46, 0x5f, 0xe1, 0x23,
	0x6d, 0x31, 0xcd, 0xa0, 0xf5, 0xa0, 0xfd, 0x34, 0x7e, 0x87, 0xb7, 0x7a, 0xc4, 0x96, 0x6f, 0xec,
	0x37, 0xe2, 0xcb, 0x00, 0x3e, 0xfb, 0x69, 0x07, 0x13, 0x56, 0x94, 0x22, 0x61, 0xe8, 0x0c, 0xc0,
	0xe7, 0x6b, 0xa7, 0xd1, 0x97, 0x1d, 0x84, 0x9b, 0xa5, 0x86, 0x8d, 0x7a, 0xad, 0xb6, 0x89, 0x7f,
	0xd8, 0x0c, 0xa2, 0xf8, 0xf8, 0xfa, 0xe6, 0x34, 0xf8, 0x14, 0x7d, 0xb0, 0x99, 0x1d, 0x5a, 0x07,
	0xdf, 0xf3, 0x42, 0xfd, 0x63, 0x89, 0xd1, 0xa4, 0x7b, 0xe4, 0x53, 0xd4, 0xbd, 0xf9, 0x1d, 0x69,
	0x0f, 0x74, 0xd1, 0x09, 0x80, 0x70, 0x50, 0x30, 0xea, 0x4f, 0xa0, 0x7d, 0x2c, 0x86, 0xfb, 0x80,
	0x22, 0xe2, 0xc4, 0x74, 0xa2, 0xf7, 0x9b, 0xc4, 0x54, 0x5a, 0xac, 0x2a, 0x17, 0x57, 0x0f, 0x74,
	0xfb, 0x17, 0x00, 0xbe, 0x4d, 0x54, 0xb6, 0x9d, 0xbb, 0xef, 0x42, 0x15, 0x92, 0x8f, 0xac, 0xf5,
	0x11, 0xf8, 0x3d, 0xac, 0xe0, 0x5c, 0xa5, 0x54, 0x72, 0xac, 0x0a, 0x4e, 0x38, 0x93, 0x2e, 0x18,
	0xe2, 0x47, 0x34, 0x17, 0xfa, 0x9e, 0x1f, 0xeb, 0xdb, 0xaa, 0x3a, 0x0f, 0x5e, 0x0c, 0x3d, 0xd3,
	0xc0, 0xf6, 0xb0, 0xfb, 0x76, 0x78, 0x1a, 0x5f, 0xd5, 0xfd, 0x99, 0xeb, 0xcf, 0x5c, 0x7f, 0x36,
	0x8d, 0xe7, 0x8f, 0xdd, 0x8d, 0xcf, 0xb7, 0x01, 0x00, 0x00, 0xff, 0xff, 0xbd, 0x94, 0x51, 0x1d,
	0x25, 0x03, 0x00, 0x00,
}
