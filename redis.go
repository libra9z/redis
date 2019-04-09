package redis

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

var client *redis.Client

//param : 0 password
func Init(addr string, db int, pools int, param ...string) {
	passwd := ""
	if len(param) > 0 {
		passwd = param[0]
	}

	client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: passwd, // no password set
		DB:       db,     // use default DB
		PoolSize: pools,
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
}

func GetIncr(key string) string {
	intCmd := client.Incr(key)

	if intCmd.Err() != nil {
		return ""
	}
	return intCmd.String()
}

func GetIncrID(key string) int64 {
	intCmd := client.Incr(key)

	if intCmd.Err() != nil {
		return 0
	}
	return intCmd.Val()
}

func GetValue(key string) string {
	result := client.Get(key)

	if result.Err() != nil {
		return ""
	}

	return result.Val()
}

func Del(key ...string) (bool, error) {
	result := client.Del(key...)
	if result.Err() != nil {
		return false, result.Err()
	}
	b := false
	if result.Val() > 0 {
		b = true
	}
	return b, nil
}

func SetValue(key string, value interface{}, exp time.Duration) {
	status := client.Set(key, value, exp)
	if status.Err() != nil {
		fmt.Printf("error: %v\n", status.Err().Error())
	}
}

func Lpush(key string, data ...interface{}) error {
	intCmd := client.LPush(key, data...)

	if intCmd.Err() != nil {
		fmt.Printf("error: %v\n", intCmd.Err().Error())
		return intCmd.Err()
	}
	return nil
}
func Brpop(timeout time.Duration, key ...string) (string, error) {
	ssliceCmd := client.BRPop(timeout, key...)
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

func Setnx(key string, value interface{}, exp time.Duration) bool {
	boolCmd := client.SetNX(key, value, exp)

	if boolCmd.Err() != nil {
		return false
	}

	return boolCmd.Val()
}

func Setex(key string, value interface{}, exp time.Duration) bool {
	statusCmd := client.Set(key, value, exp)

	if statusCmd.Err() != nil {
		fmt.Printf("error: %v\n", statusCmd.Err().Error())
		return false
	}
	return true
}

func Hget(key, field string) string {

	strCmd := client.HGet(key, field)

	if strCmd.Err() != nil {
		return ""
	}

	return strCmd.Val()
}

func Hset(key, field string, value interface{}) (bool, error) {

	boolCmd := client.HSet(key, field, value)

	return boolCmd.Val(), boolCmd.Err()
}

func Hsetnx(key, field string, value interface{}) (bool, error) {

	boolCmd := client.HSetNX(key, field, value)

	return boolCmd.Val(), boolCmd.Err()
}

func Lrange(key string, start int64, end int64) ([]string, error) {
	strCmd := client.LRange(key, start, end)
	return strCmd.Val(), strCmd.Err()
}

func Keys(pattern string) ([]string, error) {
	strCmd := client.Keys(pattern)
	return strCmd.Val(), strCmd.Err()
}

func Exists(key ...string) bool {
	intCmd := client.Exists(key...)
	if intCmd.Err() != nil {
		return false
	}
	b := false
	if intCmd.Val() > 0 {
		b = true
	}
	return b
}

func Expire(key string, exp time.Duration) bool {
	boolCmd := client.Expire(key, exp)
	if boolCmd.Err() != nil {
		return false
	}

	return boolCmd.Val()
}

func Publish(key string, buf interface{}) int64 {
	intCmd := client.Publish(key, buf)

	if intCmd.Err() != nil {
		return 0
	}

	ret := intCmd.Val()

	return ret
}
