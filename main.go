package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cos-okan/common"
)

const (
	twrConsumerCount  = 5
	twrProcessorCount = 10
)

var (
	globalMasterData common.MasterData
	rtlsRedis        *common.RtlsRedis
)

func main() {
	prepareForRun()

	twrChan := make(chan common.TwrDistance, 100)

	wg := sync.WaitGroup{}

	for i := 0; i < twrProcessorCount; i++ {
		wg.Add(1)
		go RunTwrProcessor(&wg, twrChan)
	}

	wg.Add(1)
	go runMasterDataConsumer(&wg)

	for i := 0; i < twrConsumerCount; i++ {
		wg.Add(1)
		go runTwrConsumer(&wg, twrChan)
	}

	wg.Wait()
	close(twrChan)
	// Cancel
}

func prepareForRun() {
	rtlsRedis = common.PrepareRedisClient("localhost:6379", "", 0, 3)
	loadMasterDataFromRedis(rtlsRedis)
}

func loadMasterDataFromRedis(rtlsRedis *common.RtlsRedis) {
	start := time.Now()
	globalMasterData.Anchors = rtlsRedis.GetAllAnchorsFromRedis()
	globalMasterData.Tags = rtlsRedis.GetAllTagsFromRedis()
	globalMasterData.Entities = rtlsRedis.GetAllEntitiesFromRedis()
	fmt.Println("Master data loaded from redis :", time.Since(start).Milliseconds())
}

func runMasterDataConsumer(wg *sync.WaitGroup) {
	defer wg.Done()

	topic := "master-data-update"
	brokers := []string{"localhost:19092"}
	consumerGroupID := "twr-consumer-1"
	consumer := common.NewRedpandaConsumer(brokers, topic, consumerGroupID)

	ctx := context.Background()
	for {
		fetches := consumer.Client.PollFetches(ctx)
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			mdUpdate := common.MasterDataUpdate{}
			err := mdUpdate.AvroDeserializer(record.Value)
			if err != nil {
				log.Fatal(err)
			}
			switch mdUpdate.Operation {
			case 1: //add-update
				switch mdUpdate.DataType {
				case 1: // anchor
					globalMasterData.Anchors[mdUpdate.Key] = mdUpdate.Anchor
				case 2: // tag
					globalMasterData.Tags[mdUpdate.Key] = mdUpdate.Tag
				case 3: // entity
					globalMasterData.Entities[mdUpdate.Key] = mdUpdate.Entity
				}
			case 2: //delete
				switch mdUpdate.DataType {
				case 1: // anchor
					delete(globalMasterData.Anchors, mdUpdate.Key)
				case 2: // tag
					delete(globalMasterData.Tags, mdUpdate.Key)
				case 3: // entity
					delete(globalMasterData.Entities, mdUpdate.Key)
				}
			}
		}
	}
}

func runTwrConsumer(wg *sync.WaitGroup, twrChan chan common.TwrDistance) {
	defer wg.Done()

	topic := "raw-twr"
	brokers := []string{"localhost:19092"}
	consumerGroupID := "twr-consumer-1"
	consumer := common.NewRedpandaConsumer(brokers, topic, consumerGroupID)

	ctx := context.Background()
	for {
		fetches := consumer.Client.PollFetches(ctx)
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			twr := common.TwrDistance{}
			err := twr.AvroDeserializer(record.Value)
			if err != nil {
				log.Fatal(err)
			}
			twrChan <- twr
		}
	}
}
