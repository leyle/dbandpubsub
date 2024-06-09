package main

import (
	"github.com/leyle/crud-log/pkg/crudlog"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/rs/zerolog"
	"time"
)

type CfgAndCtx struct {
	Logger *zerolog.Logger
}

// CfgAndCtx adapted standard context.Context interface, which has four methods
// below just an adaptation, not for real cases

func (c *CfgAndCtx) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, true
}

func (c *CfgAndCtx) Done() <-chan struct{} {
	return nil
}

func (c *CfgAndCtx) Err() error {
	return nil
}

func (c *CfgAndCtx) Value(key any) any {
	return c.Logger
}

func NewCfgAndCtx() *CfgAndCtx {
	initialLogger := crudlog.NewConsoleLogger(zerolog.TraceLevel)

	ctx := &CfgAndCtx{
		Logger: &initialLogger,
	}
	return ctx
}

func (c *CfgAndCtx) NewReq() *CfgAndCtx {
	// set new req id
	newC := &CfgAndCtx{}
	reqId := objectid.GetObjectId()

	l := c.Logger.With().Str("reqId", reqId).Logger()
	newC.Logger = &l

	return newC
}
