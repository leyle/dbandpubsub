package kafkaconnector

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// partitionCacheEntry stores partition count with timestamp for TTL-based expiry
type partitionCacheEntry struct {
	count    int
	cachedAt time.Time
}

type EventConnector struct {
	opt         *EventOption
	producer    *eventProducer
	consumer    *eventConsumer
	adminClient *eventAdminClient

	// partition count cache for custom partition calculation (with TTL)
	partitionCache map[string]partitionCacheEntry
	cacheLock      sync.RWMutex
}

func NewEventConnector(opt *EventOption) (*EventConnector, error) {
	parseOption(opt)

	ec := &EventConnector{
		opt:            opt,
		partitionCache: make(map[string]partitionCacheEntry),
	}

	// initial producer
	producer, err := newProducer(opt)
	if err != nil {
		return nil, err
	}
	ec.producer = producer

	// initial consumer
	consumer, err := newConsumer(opt)
	if err != nil {
		return nil, err
	}
	ec.consumer = consumer

	// initial admin client
	if opt.NeedAdmin {
		admin, err := newEventAdminClient(opt)
		if err != nil {
			return nil, err
		}
		ec.adminClient = admin
	}

	return ec, nil
}

func (ec *EventConnector) Stop(ctx context.Context) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("try to stop kafka client")

	// Signal consumer to stop gracefully first
	select {
	case ec.consumer.stopCh <- true:
		logger.Debug().Msg("sent stop signal to consumer")
	default:
		logger.Debug().Msg("stop channel already closed or full")
	}

	ec.consumer.Close()
	ec.producer.Close()
	if ec.opt.NeedAdmin {
		ec.adminClient.Close()
	}
}

func (ec *EventConnector) Reconnect(ctx context.Context) (*EventConnector, error) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("try to stop and reconnect kafka")
	ec.Stop(ctx)

	opt := ec.opt
	newEC, err := NewEventConnector(opt)
	if err != nil {
		logger.Error().Err(err).Msg("failed to reconnect to kafka")
		return nil, err
	}

	return newEC, nil
}

func parseOption(opt *EventOption) {
	opt.err = nil

	if opt.RetryCount <= 0 {
		opt.RetryCount = defaultRetryCount
	}

	if opt.RetryDelay <= time.Duration(1)*time.Second {
		opt.RetryDelay = time.Duration(defaultRetryDelay) * time.Second
	}

	if opt.Timeout <= time.Duration(1)*time.Second {
		opt.Timeout = time.Duration(defaultAdminTimeout) * time.Second
	}

	if opt.SessionTimeout <= 0 {
		opt.SessionTimeout = defaultSessionTimeout
	}

	if opt.OffsetReset == "" {
		opt.OffsetReset = defaultOffsetRest
	}

	if opt.DLQTopic == "" {
		opt.DLQTopic = defaultDQLTopic
	}

	if opt.PartitionKeyHeader == "" {
		opt.PartitionKeyHeader = defaultPartitionKeyHeader
	}

	// Set default TTL if not configured (0 means use default, negative means disabled)
	if opt.PartitionCacheTTL == 0 {
		opt.PartitionCacheTTL = defaultPartitionCacheTTL
	}
}

// getPartitionCount returns the partition count for a topic, using cache if available and not expired
func (ec *EventConnector) getPartitionCount(ctx context.Context, topic string) (int, error) {
	logger := zerolog.Ctx(ctx)

	// Check cache first (if TTL > 0)
	if ec.opt.PartitionCacheTTL > 0 {
		ec.cacheLock.RLock()
		if entry, exists := ec.partitionCache[topic]; exists {
			// Check if cache entry is still valid
			if time.Since(entry.cachedAt) < ec.opt.PartitionCacheTTL {
				ec.cacheLock.RUnlock()
				return entry.count, nil
			}
			// Cache expired, will refresh below
			logger.Debug().Str("topic", topic).Msg("partition cache expired, refreshing")
		}
		ec.cacheLock.RUnlock()
	}

	// Need admin client to query metadata
	if ec.adminClient == nil {
		logger.Warn().Msg("admin client not available, cannot get partition count")
		return 0, nil
	}

	// Query metadata from Kafka
	metadata, err := ec.adminClient.GetMetadata(&topic, false, 5000)
	if err != nil {
		logger.Error().Err(err).Str("topic", topic).Msg("failed to get topic metadata")
		return 0, err
	}

	topicMeta, exists := metadata.Topics[topic]
	if !exists {
		logger.Warn().Str("topic", topic).Msg("topic not found in metadata")
		return 0, nil
	}

	count := len(topicMeta.Partitions)

	// Cache result (if TTL > 0)
	if ec.opt.PartitionCacheTTL > 0 {
		ec.cacheLock.Lock()
		ec.partitionCache[topic] = partitionCacheEntry{
			count:    count,
			cachedAt: time.Now(),
		}
		ec.cacheLock.Unlock()
		logger.Debug().Str("topic", topic).Int("partitionCount", count).Dur("ttl", ec.opt.PartitionCacheTTL).Msg("cached partition count")
	}

	return count, nil
}
