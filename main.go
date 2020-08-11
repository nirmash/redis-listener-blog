package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws	"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var cmds []string

func getGUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

func getRedisClient() *redis.Client {
	var redisHost = os.Getenv("REDIS_MASTER_HOST") + ":" + os.Getenv("REDIS_MASTER_PORT")
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rdb
}

func InitService() {
	rdb := getRedisClient()
	rdb.ConfigSet(ctx, "notify-keyspace-events", "EA")

	err := rdb.Set(ctx, "connected", "true", 0).Err()
	if err != nil {
		panic(err)
	}

	file, err := os.Open("functionCfg")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var kName = strings.ReplaceAll(strings.Split(scanner.Text(), "|")[0], " ", "")
		var kVals = strings.ReplaceAll(strings.Split(scanner.Text(), "|")[1], " ", "")
		rdb.HSet(ctx, kName, strings.Split(kVals, ","))
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	cmds = strings.Split(os.Getenv("SUPPORTED_COMMANDS"), ",")
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func IsSupportedCommand(cmd string) bool {
	var retVal bool
	retVal = false
	for _, arrCmd := range cmds {
		if arrCmd == cmd {
			retVal = true
		}
	}
	return retVal
}

type eventMap struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	Lambda  string `json:"lambda"`
}

type MapEventList []*eventMap

var metaEvents MapEventList

type iEvent struct {
	Id       string `json:"id"`
	Obj_Name string `json:"obj_name"`
	Body     string `json:"body"`
}

func main() {
	fmt.Println("Enter init...")
	InitService()
	fmt.Println("Load map...")
	LoadExistingMap()
	fmt.Println("Listen...")
	PubSubListen()
}

func LambdaInvoke(msg string, funcName string) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	client := lambda.New(sess, &aws.Config{Region: aws.String(os.Getenv("AWS_DEFAULT_REGION"))})

	rdb := getRedisClient()
	obj := rdb.HGetAll(ctx, msg).Val()
	jsonbody, err := json.Marshal(obj)
	if err != nil {
		fmt.Println(err)
		return
	}

	payload := iEvent{Id: msg, Body: string(jsonbody), Obj_Name: msg}
	bPayload, err := json.Marshal(payload)
	fmt.Println(string(bPayload))

	result, err := client.Invoke(&lambda.InvokeInput{FunctionName: aws.String(funcName), Payload: bPayload})
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	fmt.Println(string(result.Payload))
	defer rdb.Close()
}

func LoadMetaMap(hash string) {

	rdb := getRedisClient()
	obj := rdb.HGetAll(ctx, hash).Val()
	jsonbody, err := json.Marshal(obj)
	if err != nil {
		fmt.Println(err)
		return
	}

	metaMap := eventMap{}
	if err := json.Unmarshal(jsonbody, &metaMap); err != nil {
		fmt.Println(err)
		return
	}

	metaEvents = append(metaEvents, &metaMap)
	defer rdb.Close()
}

func IsActionable(msg *redis.Message) ([]string, bool) {
	var ret []string
	var retFlg = false
	retFlg = IsSupportedCommand(strings.Split(msg.Channel, ":")[1])
	if retFlg == false {
		return ret, retFlg
	}
	for _, itm := range metaEvents {
		if itm.Pattern == "*" {
			ret = append(ret, itm.Lambda)
			retFlg = true
		} else {
			if strings.HasPrefix(msg.Payload, itm.Pattern) {
				ret = append(ret, itm.Lambda)
				retFlg = true
			}
		}
	}
	return ret, retFlg
}

func LoadExistingMap() {
	rdb := getRedisClient()

	obj := rdb.Keys(ctx, os.Getenv("META_MAP_SUFFIX")+"*").Val()
	for _, key := range obj {
		LoadMetaMap(key)
	}
	defer rdb.Close()
}

func PubSubListen() {

	rdb := getRedisClient()

	pubsub := rdb.PSubscribe(ctx, os.Getenv("REDIS_SUB_CHANNEL"))

	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	}

	ch := pubsub.Channel()

	for msg := range ch {
		fmt.Println(msg.Channel, msg.Payload)

		if strings.HasPrefix(msg.Payload, os.Getenv("META_MAP_SUFFIX")) {
			LoadMetaMap(msg.Payload)
		} else {
			checkMap, flag := IsActionable(msg)
			if flag == true {
				for _, lmbda := range checkMap {
					go LambdaInvoke(msg.Payload, lmbda)
				}
			}
		}
	}
	defer rdb.Close()
}
