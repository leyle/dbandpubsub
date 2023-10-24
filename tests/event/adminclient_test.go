package event

import (
	"github.com/leyle/dbandpubsub/kafkaconnector"
	"testing"
)

func TestAdminClient(t *testing.T) {
	baseCtx := NewCfgAndCtx()

	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		t.Fatal(err)
	}

	ctx := baseCtx.NewReq()

	err = ec.CreateTopics(ctx, consumeTopics)
	if err != nil {
		t.Fatal(err)
	}

	ec.Stop(ctx)
	t.Log("done")
}
