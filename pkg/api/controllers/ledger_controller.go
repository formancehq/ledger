package controllers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/formancehq/go-libs/api"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type LedgerController struct{}

func NewLedgerController() LedgerController {
	return LedgerController{}
}

type Info struct {
	Name    string      `json:"name"`
	Storage storageInfo `json:"storage"`
}

type storageInfo struct {
	Migrations []core.MigrationInfo `json:"migration"`
}

func (ctl *LedgerController) GetInfo(c *gin.Context) {
	l, _ := c.Get("ledger")

	var err error
	res := Info{
		Name:    c.Param("ledger"),
		Storage: storageInfo{},
	}
	res.Storage.Migrations, err = l.(*ledger.Ledger).GetMigrationsInfo(c.Request.Context())
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[Info](c, http.StatusOK, res)
}

func (ctl *LedgerController) GetStats(c *gin.Context) {
	l, _ := c.Get("ledger")

	stats, err := l.(*ledger.Ledger).Stats(c.Request.Context())
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[ledger.Stats](c, http.StatusOK, stats)
}

func (ctl *LedgerController) GetLogs(c *gin.Context) {
	l, _ := c.Get("ledger")

	var cursor api.Cursor[core.Log]
	var logsQuery *ledger.LogsQuery
	var err error

	if c.Query("pagination_token") != "" {
		if c.Query("after") != "" || c.Query("start_time") != "" ||
			c.Query("end_time") != "" || c.Query("page_size") != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				"no other query params can be set with 'pagination_token'"))
			return
		}

		res, decErr := base64.RawURLEncoding.DecodeString(c.Query("pagination_token"))
		if decErr != nil {
			apierrors.ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}

		token := sqlstorage.LogsPaginationToken{}
		if err = json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}

		logsQuery = ledger.NewLogsQuery().
			WithAfterID(token.AfterID).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithPageSize(token.PageSize)
	} else {
		var afterIDParsed uint64
		if c.Query("after") != "" {
			afterIDParsed, err = strconv.ParseUint(c.Query("after"), 10, 64)
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError("invalid query value 'after'"))
				return
			}
		}

		var startTimeParsed, endTimeParsed time.Time
		if c.Query("start_time") != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query("start_time"))
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError("invalid query value 'start_time'"))
				return
			}
		}

		if c.Query("end_time") != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query("end_time"))
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError("invalid query value 'end_time'"))
				return
			}
		}

		pageSize, err := getPageSize(c)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}

		logsQuery = ledger.NewLogsQuery().
			WithAfterID(afterIDParsed).
			WithStartTimeFilter(startTimeParsed).
			WithEndTimeFilter(endTimeParsed).
			WithPageSize(pageSize)
	}

	cursor, err = l.(*ledger.Ledger).GetLogs(c.Request.Context(), logsQuery)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithCursor[core.Log](c, http.StatusOK, cursor)
}
