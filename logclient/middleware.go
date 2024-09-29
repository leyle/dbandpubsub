package logclient

import (
	"context"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/rs/zerolog"
	"net/http"
)

const (
	headerReqIdName  = "X-REQ-ID"
	contextReqIdName = "reqId"
)

func HTTPLogMiddleware(logger zerolog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// get client request id
		reqId := request.Header.Get(headerReqIdName)
		if reqId == "" {
			// generate context request id
			reqId = objectid.GetObjectId()
		}

		// save req id into current request request
		// can be used as reqId := request.Context().Value(contextReqIdName)
		ctx := request.Context()
		ctx = context.WithValue(ctx, contextReqIdName, reqId)
		request = request.WithContext(ctx)

		// set logger req id field
		// can be used as logger := zerolog.Ctx(request.Context())
		l := logger.With().Str(contextReqIdName, reqId).Logger()
		lCtx := l.WithContext(request.Context())
		request = request.WithContext(lCtx)

		// set response headers req field
		writer.Header().Set(headerReqIdName, reqId)

		next.ServeHTTP(writer, request)
	})
}
