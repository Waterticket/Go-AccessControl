package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v9"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

func newRedisDB() {
	if rdb == nil {
		rdb = redis.NewClient(&redis.Options{
			Addr:     config.Redis.Addr,
			Password: config.Redis.Password,
			DB:       config.Redis.DB,
		})
	}

	// test connection
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal(err)
	}

	// return pong if server is online
	log.Println(pong)

	setFirstValue()
}

func setFirstValue() {
	if rdb.Exists(ctx, "accesscontrol:current_idx").Val() == 0 {
		err := rdb.Set(ctx, "accesscontrol:current_idx", 0, 0).Err()
		if err != nil {
			log.Fatal(err)
		}
	}

	if rdb.Exists(ctx, "accesscontrol:last_processed_idx").Val() == 0 {
		err := rdb.Set(ctx, "accesscontrol:last_processed_idx", 0, 0).Err()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getNextIdx() int64 {
	// get next index
	idx, err := rdb.Incr(ctx, "accesscontrol:current_idx").Result()
	if err != nil {
		log.Fatal(err)
	}

	return idx
}

func getCurrentIdx() int64 {
	// get current index
	idx, err := rdb.Get(ctx, "accesscontrol:current_idx").Int64()
	if err != nil {
		log.Fatal(err)
	}

	return idx
}

func getLastProcessedIdx() int64 {
	// get last processed index
	idx, err := rdb.Get(ctx, "accesscontrol:last_processed_idx").Int64()
	if err != nil {
		log.Fatal(err)
	}

	return idx
}

func setLastProcessedIdx(idx int64) {
	lastProcessedIdx := getLastProcessedIdx()
	if lastProcessedIdx > idx {
		return
	}

	err := rdb.Set(ctx, "accesscontrol:last_processed_idx", idx, 0).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func popPendingQueue() string {
	// pop pending queue
	token, err := rdb.RPop(ctx, "accesscontrol:pending_queue").Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return token
}

func pushPendingQueue(token string) {
	// push pending queue
	err := rdb.LPush(ctx, "accesscontrol:pending_queue", token).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func zaddBucket(token string) {
	// add to bucket
	var timestamp = time.Now().Unix()
	err := rdb.ZAdd(ctx, "accesscontrol:bucket", redis.Z{
		Score:  float64(timestamp + config.Connection.TTL),
		Member: token,
	}).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func zscoreBucket(token string) int64 {
	// get score from bucket
	score, err := rdb.ZScore(ctx, "accesscontrol:bucket", token).Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return int64(score)
}

func zremBucket(token string) {
	// remove from bucket
	err := rdb.ZRem(ctx, "accesscontrol:bucket", token).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func zrangebyscoreBucket() []string {
	// get range of bucket
	tokens, err := rdb.ZRangeByScore(ctx, "accesscontrol:bucket", &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", time.Now().Unix()),
	}).Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return tokens
}

func zremrangebyscoreBucket() int64 {
	// remove from bucket
	count, err := rdb.ZRemRangeByScore(ctx, "accesscontrol:bucket", "-inf", fmt.Sprintf("%d", time.Now().Unix())).Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return count
}

func getCountOfBucket() int64 {
	// get count of bucket
	count, err := rdb.ZCard(ctx, "accesscontrol:bucket").Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return count
}

func getCountOfPendingQueue() int64 {
	// get count of pending queue
	count, err := rdb.LLen(ctx, "accesscontrol:pending_queue").Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return count
}
