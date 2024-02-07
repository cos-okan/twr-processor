package main

import (
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/cos-okan/common"
)

func RunTwrProcessor(wg *sync.WaitGroup, twrChan chan common.TwrDistance) {
	defer wg.Done()

	topic := "proc-twr"
	brokers := []string{"localhost:19092"}
	rp := common.NewRedpandaProducer(brokers, topic)

	for twr := range twrChan {
		pd := processTwrDistance(twr, globalMasterData)
		if !pd.IsInvalid {
			rp.SendAvroMessage(&pd)
		}
	}
}

func processTwrDistance(twr common.TwrDistance, md common.MasterData) common.ProcessedDistance {
	p := common.ProcessedDistance{Twr: twr}
	fillDeviceInfo(md, &p)
	deviceValidation(&p)
	calculateProjectionDistance(&p)
	shortDistanceValidation(&p)
	anchorRangeValidation(&p)
	return p
}

func fillDeviceInfo(md common.MasterData, p *common.ProcessedDistance) {
	if p.IsInvalid {
		return
	}

	tagKey := fmt.Sprintf("tag:%d", p.Twr.FromNodeId)
	entityID, found := md.Tags[tagKey]
	if !found {
		log.Printf("Tag not found in master data for tag : %d \n", p.Twr.FromNodeId)
		p.IsUndefined = true
		return
	}

	eKey := fmt.Sprintf("entity:%d", entityID)
	entity, found := md.Entities[eKey]
	if !found {
		log.Printf("Entity not found in master data for entity : %d \n", entityID)
		p.IsUndefined = true
		return
	}
	p.Entity = entity

	anchorKey := fmt.Sprintf("anchor:%d", p.Twr.ToNodeId)
	anchor, found := md.Anchors[anchorKey]
	if !found {
		log.Printf("Anchor not found in master data for anchor : %d \n", p.Twr.ToNodeId)
		p.IsUndefined = true
		return
	}
	p.Anchor = anchor
}

func deviceValidation(p *common.ProcessedDistance) {
	if p.IsInvalid {
		return
	}

	if p.IsUndefined {
		makeInvalid(p)
	}
}

func calculateProjectionDistance(p *common.ProcessedDistance) {
	if p.IsInvalid {
		return
	}

	heightDiff := math.Abs(float64(p.Entity.Height) - float64(p.Anchor.Location.Point.Z))
	hypoAltDiff := p.Twr.Distance - int(heightDiff)
	if hypoAltDiff < -100 { // TODO: should be configurable
		p.IsShort = true
	} else if hypoAltDiff < 0 { // TODO: should be configurable
		p.ProjectionDistance = 0
		p.ConfidenceLevel = 100
		p.OnAnchor = true
	} else {
		p.ProjectionDistance = int(math.Sqrt(math.Pow(float64(p.Twr.Distance), 2) - math.Pow(heightDiff, 2)))
		if p.ProjectionDistance < 50 { // TODO: should be configurable
			p.ConfidenceLevel = 100
			p.OnAnchor = true
		} else if p.Twr.Distance < int(heightDiff*1.25) { // TODO: should be analyzed
			p.ConfidenceLevel = 50
		} else {
			p.ConfidenceLevel = 75
		}
	}
}

func shortDistanceValidation(p *common.ProcessedDistance) {
	if p.IsInvalid {
		return
	}

	if p.IsShort {
		makeInvalid(p)
	}
}

func anchorRangeValidation(p *common.ProcessedDistance) {
	if p.IsInvalid {
		return
	}

	if p.Anchor.Range < p.ProjectionDistance {
		p.IsOutOfRange = true
	}
}

func makeInvalid(p *common.ProcessedDistance) {
	p.IsInvalid = true
	p.ConfidenceLevel = 0
}
