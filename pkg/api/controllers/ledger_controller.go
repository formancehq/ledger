package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

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
	Migrations []core.MigrationInfo `json:"migrations"`
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

	logsQuery := ledger.NewLogsQuery()

	if c.Query(QueryKeyCursor) != "" {
		if c.Query("after") != "" ||
			c.Query(QueryKeyStartTime) != "" ||
			c.Query(QueryKeyStartTimeDeprecated) != "" ||
			c.Query(QueryKeyEndTime) != "" ||
			c.Query(QueryKeyEndTimeDeprecated) != "" ||
			c.Query(QueryKeyPageSize) != "" ||
			c.Query(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(c.Query(QueryKeyCursor))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		token := sqlstorage.LogsPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		logsQuery = logsQuery.
			WithAfterID(token.AfterID).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithPageSize(token.PageSize)

	} else if c.Query(QueryKeyCursorDeprecated) != "" {
		if c.Query("after") != "" ||
			c.Query(QueryKeyStartTime) != "" ||
			c.Query(QueryKeyStartTimeDeprecated) != "" ||
			c.Query(QueryKeyEndTime) != "" ||
			c.Query(QueryKeyEndTimeDeprecated) != "" ||
			c.Query(QueryKeyPageSize) != "" ||
			c.Query(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursorDeprecated)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(c.Query(QueryKeyCursorDeprecated))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		token := sqlstorage.LogsPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		logsQuery = logsQuery.
			WithAfterID(token.AfterID).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithPageSize(token.PageSize)

	} else {
		var err error
		var afterIDParsed uint64
		if c.Query("after") != "" {
			afterIDParsed, err = strconv.ParseUint(c.Query("after"), 10, 64)
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError(
					"invalid 'after' query param"))
				return
			}
		}

		var startTimeParsed, endTimeParsed time.Time
		if c.Query(QueryKeyStartTime) != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyStartTime))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidStartTime)
				return
			}
		}
		if c.Query(QueryKeyStartTimeDeprecated) != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyStartTimeDeprecated))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidStartTimeDeprecated)
				return
			}
		}

		if c.Query(QueryKeyEndTime) != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyEndTime))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidEndTime)
				return
			}
		}
		if c.Query(QueryKeyEndTimeDeprecated) != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyEndTimeDeprecated))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidEndTimeDeprecated)
				return
			}
		}

		pageSize, err := getPageSize(c)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}

		logsQuery = logsQuery.
			WithAfterID(afterIDParsed).
			WithStartTimeFilter(startTimeParsed).
			WithEndTimeFilter(endTimeParsed).
			WithPageSize(pageSize)
	}

	cursor, err := l.(*ledger.Ledger).GetLogs(c.Request.Context(), logsQuery)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithCursor[core.Log](c, http.StatusOK, cursor)
}
