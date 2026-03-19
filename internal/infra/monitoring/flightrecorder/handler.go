package flightrecorder

import (
	"net/http"
	"time"
)

// SnapshotHandler returns an HTTP handler that writes a flight recorder
// snapshot as a downloadable .trace file.
func SnapshotHandler(recorder *Recorder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !recorder.Enabled() {
			http.Error(w, "flight recorder is not running", http.StatusServiceUnavailable)

			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename=snapshot-"+time.Now().UTC().Format("20060102-150405")+".trace")

		if err := recorder.Snapshot(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	}
}
