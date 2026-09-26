package registry

import (
	"encoding/json"
	"net/http"

	"connectrpc.com/connect"
)

// Service procedure paths, matching api/registry/v1/registry.proto.
const (
	ProcedureRegisterVersion    = "/registry.v1.Registry/RegisterVersion"
	ProcedureCheckCompatibility = "/registry.v1.Registry/CheckCompatibility"
	ProcedureDeclareConsumer    = "/registry.v1.Registry/DeclareConsumer"
	ProcedureListVersions       = "/registry.v1.Registry/ListVersions"
)

// Corpus service procedure paths (registry.v1.Corpus).
const (
	ProcedureCreateCorpus      = "/registry.v1.Corpus/CreateCorpus"
	ProcedureUpdateCorpus      = "/registry.v1.Corpus/UpdateCorpus"
	ProcedureSealCorpus        = "/registry.v1.Corpus/SealCorpus"
	ProcedureGetCorpus         = "/registry.v1.Corpus/GetCorpus"
	ProcedureListCorpora       = "/registry.v1.Corpus/ListCorpora"
	ProcedureDeleteCorpusDraft = "/registry.v1.Corpus/DeleteCorpusDraft"
	ProcedureStartReplay       = "/registry.v1.Corpus/StartReplay"
	ProcedureGetReplay         = "/registry.v1.Corpus/GetReplay"
	ProcedureListReplays       = "/registry.v1.Corpus/ListReplays"
	ProcedureCompareReplays    = "/registry.v1.Corpus/CompareReplays"
)

// jsonCodec speaks application/json for plain Go structs, so the service
// needs no code generation step while remaining a first-class ConnectRPC
// endpoint (connect protocol, and gRPC/gRPC-Web with the json sub-codec).
type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Handler returns the HTTP handler for the registry service. Procedures are
// registered at their absolute paths on one flat mux; the caller may mount
// additional non-overlapping paths (e.g. "/healthz") on the same mux. Do
// not nest the handler under a subtree prefix — Go's ServeMux strips the
// matched prefix before delegating.
func (s *Service) Handler() http.Handler {
	return s.handler(nil)
}

// CorpusHandler returns an HTTP handler exposing both the schema registry
// and the versioned-corpus service.
func CorpusHandler(schema *Service, corpus *CorpusService) http.Handler {
	return schema.handler(corpus)
}

// NewRootMux returns a flat root mux containing the schema and corpus
// procedures plus the given extra paths (e.g. "/healthz"). All procedure
// paths live on this one mux: Go's ServeMux strips a matched subtree
// prefix before delegating, so nesting the handlers under "/registry.v1/"
// would 404 the exact procedure paths.
func (s *Service) NewRootMux(corpus *CorpusService, extras map[string]http.HandlerFunc) http.Handler {
	mux := s.handler(corpus).(*http.ServeMux)
	for pattern, h := range extras {
		mux.HandleFunc(pattern, h)
	}
	return mux
}

func (s *Service) handler(corpus *CorpusService) http.Handler {
	mux := http.NewServeMux()
	codec := connect.WithCodec(jsonCodec{})
	mux.Handle(ProcedureRegisterVersion, connect.NewUnaryHandler(
		ProcedureRegisterVersion, s.RegisterVersion, codec))
	mux.Handle(ProcedureCheckCompatibility, connect.NewUnaryHandler(
		ProcedureCheckCompatibility, s.CheckCompatibility, codec))
	mux.Handle(ProcedureDeclareConsumer, connect.NewUnaryHandler(
		ProcedureDeclareConsumer, s.DeclareConsumer, codec))
	mux.Handle(ProcedureListVersions, connect.NewUnaryHandler(
		ProcedureListVersions, s.ListVersions, codec))

	if corpus != nil {
		mux.Handle(ProcedureCreateCorpus, connect.NewUnaryHandler(
			ProcedureCreateCorpus, corpus.CreateCorpus, codec))
		mux.Handle(ProcedureUpdateCorpus, connect.NewUnaryHandler(
			ProcedureUpdateCorpus, corpus.UpdateCorpus, codec))
		mux.Handle(ProcedureSealCorpus, connect.NewUnaryHandler(
			ProcedureSealCorpus, corpus.SealCorpus, codec))
		mux.Handle(ProcedureGetCorpus, connect.NewUnaryHandler(
			ProcedureGetCorpus, corpus.GetCorpus, codec))
		mux.Handle(ProcedureListCorpora, connect.NewUnaryHandler(
			ProcedureListCorpora, corpus.ListCorpora, codec))
		mux.Handle(ProcedureDeleteCorpusDraft, connect.NewUnaryHandler(
			ProcedureDeleteCorpusDraft, corpus.DeleteCorpusDraft, codec))
		mux.Handle(ProcedureStartReplay, connect.NewUnaryHandler(
			ProcedureStartReplay, corpus.StartReplay, codec))
		mux.Handle(ProcedureGetReplay, connect.NewUnaryHandler(
			ProcedureGetReplay, corpus.GetReplay, codec))
		mux.Handle(ProcedureListReplays, connect.NewUnaryHandler(
			ProcedureListReplays, corpus.ListReplays, codec))
		mux.Handle(ProcedureCompareReplays, connect.NewUnaryHandler(
			ProcedureCompareReplays, corpus.CompareReplays, codec))
	}
	return mux
}
