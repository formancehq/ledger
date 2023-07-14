package query

import (
	"context"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/utils/job"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type metadataUpdaterInput struct {
	id any
	metadata.Metadata
	callback func()
}

type inputs []*metadataUpdaterInput

func (inputs inputs) aggregated() metadata.Metadata {
	m := metadata.Metadata{}
	for _, object := range inputs {
		m = m.Merge(object.Metadata)
	}
	return m
}

type metadataUpdaterBuffer struct {
	id     any
	inputs inputs
}

type metadataUpdaterJob struct {
	updater    *metadataUpdater
	buffers    []*metadataUpdaterBuffer
	inputs     []*metadataUpdaterInput
	aggregated []*MetadataUpdate
}

func (j metadataUpdaterJob) String() string {
	return fmt.Sprintf("inserting %d objects", len(j.inputs))
}

func (j metadataUpdaterJob) Terminated() {
	for _, input := range j.inputs {
		input.callback()
	}

	j.updater.mu.Lock()
	defer j.updater.mu.Unlock()

	for _, buffer := range j.buffers {
		if len(buffer.inputs) == 0 {
			delete(j.updater.objects, buffer.id)
		} else {
			j.updater.queue.Append(buffer)
		}
	}
}

type metadataUpdater struct {
	*job.Runner[metadataUpdaterJob]
	queue         *collectionutils.LinkedList[*metadataUpdaterBuffer]
	objects       map[any]*metadataUpdaterBuffer
	input         chan *moveBufferInput
	mu            sync.Mutex
	maxBufferSize int
}

func (r *metadataUpdater) Append(id any, metadata metadata.Metadata, callback func()) {
	r.mu.Lock()

	mba, ok := r.objects[id]
	if !ok {
		mba = &metadataUpdaterBuffer{
			id: id,
		}
		r.objects[id] = mba
		r.queue.Append(mba)
	}
	mba.inputs = append(mba.inputs, &metadataUpdaterInput{
		id:       id,
		Metadata: metadata,
		callback: callback,
	})
	r.mu.Unlock()

	r.Runner.Next()
}

func (r *metadataUpdater) nextJob() *metadataUpdaterJob {
	r.mu.Lock()
	defer r.mu.Unlock()

	batch := make([]*metadataUpdaterInput, 0)
	aggregated := make([]*MetadataUpdate, 0)
	for len(batch) < r.maxBufferSize {
		mba := r.queue.TakeFirst()
		if mba == nil {
			break
		}

		batch = append(batch, mba.inputs...)
		aggregated = append(aggregated, &MetadataUpdate{
			ID:       mba.id,
			Metadata: mba.inputs.aggregated(),
		})
		mba.inputs = inputs{}
	}

	if len(batch) == 0 {
		return nil
	}

	return &metadataUpdaterJob{
		inputs:     batch,
		updater:    r,
		aggregated: aggregated,
	}
}

type MetadataUpdate struct {
	ID       any
	Metadata metadata.Metadata
}

func newMetadataUpdater(runner func(context.Context, ...*MetadataUpdate) error, nbWorkers, maxBufferSize int) *metadataUpdater {
	ret := &metadataUpdater{
		queue:         collectionutils.NewLinkedList[*metadataUpdaterBuffer](),
		objects:       map[any]*metadataUpdaterBuffer{},
		input:         make(chan *moveBufferInput),
		maxBufferSize: maxBufferSize,
	}
	ret.Runner = job.NewJobRunner[metadataUpdaterJob](func(ctx context.Context, job *metadataUpdaterJob) error {
		return runner(ctx, job.aggregated...)
	}, ret.nextJob, nbWorkers)
	return ret
}
