package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
	"strconv"
)

var client *redis.Client

//param : [0] password
//param : [1] mode: alone,sentinel,cluster
//param : [2] mastername,
func Init(addr string, db int, pools int, param ...string) {
	passwd := ""
	if len(param) > 0 {
		passwd = param[0]
	}
	mode :="alone"
	if len(param) >1 {
		mode = param[1]
	}
	mastername :="mymaster"
	if len(param) >2 {
		mastername = param[2]
	}

	var address []string

	address = strings.Split(addr,",")

	switch mode {
	case "alone":
		client = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: passwd, // no password set
			DB:       db,     // use default DB
			PoolSize: pools,
		})
	case "sentinel":
		sf := &redis.FailoverOptions{
			// The master name.
			MasterName: mastername,
			// A seed list of host:port addresses of sentinel nodes.
			SentinelAddrs: address,

			// Following options are copied from Options struct.
			Password: passwd,
			DB:       db,
		}
		client = redis.NewFailoverClient(sf)
	}
	ctx := context.Background()
	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)
}

func NewRedisClient(addr string, db int, pools int, param ...string) (c *redis.Client) {
	passwd := ""
	if len(param) > 0 {
		passwd = param[0]
	}
	mode :="alone"
	if len(param) >1 {
		mode = param[1]
	}
	mastername :="mymaster"
	if len(param) >2 {
		mastername = param[2]
	}

	var address []string

	address = strings.Split(addr,",")

	switch mode {
	case "alone":
		c = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: passwd, // no password set
			DB:       db,     // use default DB
			PoolSize: pools,
		})
	case "sentinel":
		sf := &redis.FailoverOptions{
			// The master name.
			MasterName: mastername,
			// A seed list of host:port addresses of sentinel nodes.
			SentinelAddrs: address,

			// Following options are copied from Options struct.
			Password: passwd,
			DB:       db,
		}
		c = redis.NewFailoverClient(sf)
	}
	if c == nil {
		return nil
	}
	ctx := context.Background()
	pong, err := c.Ping(ctx).Result()
	fmt.Println(pong, err)
	return c
}

func GetIncr(ctx context.Context,key string) string {
	intCmd := client.Incr(ctx,key)

	if intCmd.Err() != nil {
		return ""
	}

	return strconv.FormatInt(intCmd.Val(),10)
}

func GetIncrID(ctx context.Context,key string) int64 {
	intCmd := client.Incr(ctx,key)

	if intCmd.Err() != nil {
		return 0
	}
	return intCmd.Val()
}

func GetValue(ctx context.Context,key string) string {
	result := client.Get(ctx,key)

	if result.Err() != nil {
		return ""
	}

	return result.Val()
}

func Del(ctx context.Context,key ...string) (bool, error) {
	result := client.Del(ctx,key...)
	if result.Err() != nil {
		return false, result.Err()
	}
	b := false
	if result.Val() > 0 {
		b = true
	}
	return b, nil
}

func SetValue(ctx context.Context,key string, value interface{}, exp time.Duration) {
	status := client.Set(ctx,key, value, exp)
	if status.Err() != nil {
		fmt.Printf("error: %v\n", status.Err().Error())
	}
}

func Lpush(ctx context.Context,key string, data ...interface{}) error {
	intCmd := client.LPush(ctx,key, data...)

	if intCmd.Err() != nil {
		fmt.Printf("error: %v\n", intCmd.Err().Error())
		return intCmd.Err()
	}
	return nil
}
func Brpop(ctx context.Context,timeout time.Duration, key ...string) (string, error) {
	ssliceCmd := client.BRPop(ctx,timeout, key...)
	if ssliceCmd.Err() != nil {
		return "", ssliceCmd.Err()
	}
	if key == nil {
		return "", errors.New("get empty data")
	}
	ret := ""
	if len(ssliceCmd.Val()) > 1 {
		ret = ssliceCmd.Val()[1]
	}
	return ret, nil
}

func Setnx(ctx context.Context,key string, value interface{}, exp time.Duration) bool {
	boolCmd := client.SetNX(ctx,key, value, exp)

	if boolCmd.Err() != nil {
		return false
	}

	return boolCmd.Val()
}

func Setex(ctx context.Context,key string, value interface{}, exp time.Duration) bool {
	statusCmd := client.Set(ctx,key, value, exp)

	if statusCmd.Err() != nil {
		fmt.Printf("error: %v\n", statusCmd.Err().Error())
		return false
	}
	return true
}

func Hget(ctx context.Context,key, field string) string {

	strCmd := client.HGet(ctx,key, field)

	if strCmd.Err() != nil {
		return ""
	}

	return strCmd.Val()
}

func Hset(ctx context.Context,key, field string, value interface{}) (int64, error) {

	boolCmd := client.HSet(ctx,key, field, value)

	return boolCmd.Val(), boolCmd.Err()
}

func Hsetnx(ctx context.Context,key, field string, value interface{}) (bool, error) {

	boolCmd := client.HSetNX(ctx,key, field, value)

	return boolCmd.Val(), boolCmd.Err()
}

func Lrange(ctx context.Context,key string, start int64, end int64) ([]string, error) {
	strCmd := client.LRange(ctx,key, start, end)
	return strCmd.Val(), strCmd.Err()
}

func Keys(ctx context.Context,pattern string) ([]string, error) {
	strCmd := client.Keys(ctx,pattern)
	return strCmd.Val(), strCmd.Err()
}

func Exists(ctx context.Context,key ...string) bool {
	intCmd := client.Exists(ctx,key...)
	if intCmd.Err() != nil {
		return false
	}
	b := false
	if intCmd.Val() > 0 {
		b = true
	}
	return b
}

func Expire(ctx context.Context,key string, exp time.Duration) bool {
	boolCmd := client.Expire(ctx,key, exp)
	if boolCmd.Err() != nil {
		return false
	}

	return boolCmd.Val()
}

func Publish(ctx context.Context,key string, buf interface{}) int64 {
	intCmd := client.Publish(ctx,key, buf)

	if intCmd.Err() != nil {
		return 0
	}

	ret := intCmd.Val()

	return ret
}

func TTL(ctx context.Context,key string) time.Duration {
	duCmd := client.TTL(ctx,key)
	if duCmd.Err() != nil {
		return 0
	}

	return duCmd.Val()
}

func FetchMessage(ctx context.Context,dq string, timeout uint) ([]string, error) {
	var queues []string
	queues = append(queues, dq)
	strs := client.BRPop(ctx,time.Duration(timeout)*time.Second,queues...)

	if strs == nil {
		return nil, errors.New("get empty data")
	}
	
	return strs.Val(), nil
}

func Hgetall(ctx context.Context,key string, val interface{}) error {
	duCmd := client.HGetAll(ctx,key)
	if duCmd.Err() != nil {
		return duCmd.Err()
	}
	val,err := duCmd.Result()
	return err
}

func Sadd(ctx context.Context,key string, members... interface{}) (interface{},error) {
	duCmd := client.SAdd(ctx,key,members...)
	if duCmd.Err() != nil {
		return 0,duCmd.Err()
	}
	val,err := duCmd.Result()

	return val,err
}

func Sismember(ctx context.Context,key string, members interface{}) (bool,error) {
	bCmd := client.SIsMember(ctx,key,members)
	if bCmd.Err() != nil {
		return false,bCmd.Err()
	}
	val,err := bCmd.Result()

	return val,err
}

