package pagination

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"
)

const (
	Asc   = "ASC"
	Desc  = "DESC"
	First = "FIRST"
	Last  = "LAST"
)

type OrderByColumn struct {
	SortExpresssion string
	Direction       string // ASC or DESC
	NullOption      string // FIRST or LAST
	// Used to get the value of sort expression from a given record
	GetValueFromRecord func(interface{}) interface{}
}

func (c *OrderByColumn) String() string {
	result := c.SortExpresssion
	if c.Direction != "" {
		result = fmt.Sprintf("%s %s", result, c.Direction)
	}
	if c.NullOption != "" {
		result = fmt.Sprintf("%s NULLS %s", result, c.NullOption)
	}
	return result
}

type Condition struct {
	SQL    string        // like "A = ? AND B > ?"
	Values []interface{} // like []interface{}{20, "2020-01-01"}
}

func (c *Condition) mergeValues(values []interface{}) {
	if len(values) > 0 {
		c.Values = append(c.Values, values...)
	}
}

func NextPageConditon(
	columns []OrderByColumn, // the definition of ORDER BY columns
	values []interface{}, // the values of the last row of the last page
) Condition {
	column := columns[0]
	// The value of column.SortExpression in the last row of the last page
	prevValue := values[0]

	sign := "<"
	if column.Direction == Asc {
		sign = ">"
	}

	if len(columns) == 1 {
		condition := Condition{SQL: "", Values: make([]interface{}, 0, len(columns))}

		switch {
		case prevValue == nil && column.NullOption == Last:
			// No value after NULL
			condition.SQL = fmt.Sprintf("(%s IS NOT NULL AND %s IS NULL)", column.SortExpresssion, column.SortExpresssion)
		case prevValue == nil && column.NullOption == First:
			condition.SQL = fmt.Sprintf("(%s IS NOT NULL)", column.SortExpresssion)
		case prevValue != nil && column.NullOption == Last:
			// the next row could be NULL
			condition.SQL = fmt.Sprintf("((%s %s ?) OR (%s IS NULL))", column.SortExpresssion, sign, column.SortExpresssion)
			condition.Values = []interface{}{prevValue}
		case prevValue != nil && column.NullOption == First:
			condition.SQL = fmt.Sprintf("(%s %s ?)", column.SortExpresssion, sign)
			condition.Values = []interface{}{prevValue}
		}
		return condition
	} else {
		condition := NextPageConditon(columns[1:], values[1:])
		var newCondition Condition
		switch {
		case prevValue == nil && column.NullOption == Last:
			newCondition.SQL = fmt.Sprintf("((%s IS NULL) AND %s)", column.SortExpresssion, condition.SQL)
			newCondition.mergeValues(condition.Values)
		case prevValue == nil && column.NullOption == First:
			newCondition.SQL = fmt.Sprintf("((%s IS NOT NULL) OR ((%s IS NULL) AND %s))", column.SortExpresssion, column.SortExpresssion, condition.SQL)
			newCondition.mergeValues(condition.Values)
		case prevValue != nil && column.NullOption == Last:
			// the next row could be NULL
			newCondition.SQL = fmt.Sprintf(
				"(((%s %s ?) OR (%s IS NULL)) OR ((%s = ?) AND %s))",
				column.SortExpresssion, sign, column.SortExpresssion, column.SortExpresssion, condition.SQL,
			)
			newCondition.mergeValues([]interface{}{prevValue, prevValue})
			newCondition.mergeValues(condition.Values)
		case prevValue != nil && column.NullOption == First:
			newCondition.SQL = fmt.Sprintf("((%s %s ?) OR ((%s = ?) AND %s))", column.SortExpresssion, sign, column.SortExpresssion, condition.SQL)
			newCondition.mergeValues([]interface{}{prevValue, prevValue})
			newCondition.mergeValues(condition.Values)
		}
		return newCondition
	}
}

func PaginatedQuery[T any](
	ctx context.Context,
	dest *[]T,
	db *gorm.DB,
	queryWithDB func(*gorm.DB) *gorm.DB,
	pageSize int, // find all records if pageSize == 0
	pageToken string,
	orderByColumns []OrderByColumn,
) (string, error) {
	// first, decode page token
	var paginationCondition Condition
	if pageToken != "" {
		lastCoachingPlanOrderByValues, err := decodeNextPageToken(pageToken)
		if err != nil {
			return "", err
		}
		paginationCondition = NextPageConditon(orderByColumns, lastCoachingPlanOrderByValues)
	}

	// second, construct the query with pagination and page size
	wrapperQueryWithDB := func(db *gorm.DB) *gorm.DB {
		query := queryWithDB(db).
			Scopes(orderByScope(orderByColumns...))
		if pageToken != "" {
			query = query.Where(paginationCondition.SQL, paginationCondition.Values...)
		}
		if pageSize > 0 {
			query = query.Limit(pageSize + 1)
		}
		query = query.Find(&dest)
		return query
	}

	// execute the paginated query
	err := wrapperQueryWithDB(db).Error
	if err != nil {
		return "", err
	}

	// encode the next page token if necessary
	nextPageToken := ""
	if pageSize > 0 && len(*dest) > pageSize {
		*dest = (*dest)[:pageSize]
		lastRecord := (*dest)[pageSize-1]
		valuesForToken := []interface{}{}
		for _, orderByColumn := range orderByColumns {
			value := orderByColumn.GetValueFromRecord(lastRecord)
			valuesForToken = append(valuesForToken, value)
		}
		nextPageToken, err = encodeNextPageToken(valuesForToken)
		if err != nil {
			return "", err
		}
	}
	return nextPageToken, nil
}

// Order the query results
func orderByScope(columns ...OrderByColumn) func(*gorm.DB) *gorm.DB {
	order := ""
	for i, c := range columns {
		if i == 0 {
			order = fmt.Sprintf("%s %s NULLS %s", c.SortExpresssion, c.Direction, c.NullOption)
		} else {
			order = fmt.Sprintf("%s, %s %s NULLS %s", order, c.SortExpresssion, c.Direction, c.NullOption)
		}
	}
	return func(db *gorm.DB) *gorm.DB {
		return db.Order(order)
	}
}

type PageToken struct {
	OrderColumnValues []interface{}
}

// base64 encode the json page token
func encodeNextPageToken(orderColumnValues []interface{}) (string, error) {
	token := PageToken{
		OrderColumnValues: orderColumnValues,
	}

	encoded, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

// Decode the next page token
func decodeNextPageToken(nextPageToken string) ([]interface{}, error) {
	decoded, err := base64.StdEncoding.DecodeString(nextPageToken)
	if err != nil {
		return nil, err
	}
	var token PageToken
	err = json.Unmarshal(decoded, &token)
	if err != nil {
		return nil, err
	}
	return token.OrderColumnValues, nil
}
