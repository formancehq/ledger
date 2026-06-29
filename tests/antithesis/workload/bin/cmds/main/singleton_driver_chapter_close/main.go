package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: singleton_driver_chapter_close")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	// Chapter lifecycle loop:
	// 1. Close the current chapter (triggers checkpoint)
	// 2. Find CLOSED chapters (sealed by the leader automatically)
	// 3. Archive them (uploads to cold storage / S3)
	// 4. Confirm the archive
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		closeChapter(ctx, client)
		archiveClosedChapters(ctx, client)
	}
}

func closeChapter(ctx context.Context, client servicepb.BucketServiceClient) {
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_CloseChapter{
			CloseChapter: &servicepb.CloseChapterRequest{},
		},
	}))

	if err != nil {
		if internal.IsTransient(err) {
			log.Printf("chapter close unavailable: %s", err)
			return
		}

		assert.Unreachable("chapter close returned unexpected error", internal.Details{"error": err})

		return
	}

	assert.Reachable("chapter close succeeded", nil)
	log.Println("chapter closed successfully")
}

func archiveClosedChapters(ctx context.Context, client servicepb.BucketServiceClient) {
	chapters, err := listChapters(ctx, client)
	if err != nil {
		return
	}

	for _, p := range chapters {
		if p.GetStatus() != commonpb.ChapterStatus_CHAPTER_CLOSED {
			continue
		}

		chapterID := p.GetId()
		details := internal.Details{"chapterId": chapterID}

		// Archive the closed chapter (uploads logs to cold storage).
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_ArchiveChapter{
				ArchiveChapter: &servicepb.ArchiveChapterRequest{
					ChapterId: chapterID,
				},
			},
		}))

		if err != nil {
			if internal.IsTransient(err) {
				continue
			}

			log.Printf("archive chapter %d failed: %s", chapterID, err)

			continue
		}

		// Confirm the archive (purges hot data).
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_ConfirmArchiveChapter{
				ConfirmArchiveChapter: &servicepb.ConfirmArchiveChapterRequest{
					ChapterId: chapterID,
				},
			},
		}))

		if err != nil {
			if internal.IsTransient(err) {
				continue
			}

			log.Printf("confirm archive chapter %d failed: %s", chapterID, err)

			continue
		}

		assert.Reachable("chapter archive completed", details)
		log.Printf("chapter %d archived and confirmed", chapterID)
	}
}

func listChapters(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Chapter, error) {
	stream, err := client.ListChapters(ctx, &servicepb.ListChaptersRequest{})
	if err != nil {
		return nil, err
	}

	var chapters []*commonpb.Chapter

	for {
		p, err := stream.Recv()
		if err == io.EOF {
			return chapters, nil
		}
		if err != nil {
			return chapters, err
		}

		chapters = append(chapters, p)
	}
}
