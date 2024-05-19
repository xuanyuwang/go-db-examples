package pagination

import (
	"fmt"
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

func NextPage(
	columns []OrderByColumn, // the definition of ORDER BY columns
	values []interface{}, // the values of the last row of the last page
) Condition {
	allRecords, excludeRecord := generatePaginationForColumn(columns, values)
	condition := Condition{
		SQL: fmt.Sprintf("%s AND (NOT %s)", allRecords.SQL, excludeRecord.SQL),
	}
	condition.mergeValues(allRecords.Values)
	condition.mergeValues(excludeRecord.Values)
	return condition
}

func generatePaginationForColumn(
	columns []OrderByColumn, // the definition of ORDER BY columns
	values []interface{}, // the values of the last row of the last page
) (Condition, Condition) {
	column := columns[0]
	// The value of column.SortExpression in the last row of the last page
	prevValue := values[0]

	sign := "<"
	if column.Direction == Asc {
		sign = ">"
	}

	if len(columns) == 1 {
		condition := Condition{SQL: "", Values: make([]interface{}, 0, len(columns))}
		excludeValue := Condition{SQL: "", Values: make([]interface{}, 0, len(columns))}

		switch {
		case prevValue == nil && column.NullOption == Last:
			condition.SQL = fmt.Sprintf("(%s IS NULL)", column.SortExpresssion)
			excludeValue.SQL = fmt.Sprintf("(%s IS NULL)", column.SortExpresssion)
		case prevValue == nil && column.NullOption == First:
			condition.SQL = fmt.Sprintf("((%s IS NULL) OR (%s IS NOT NULL))", column.SortExpresssion, column.SortExpresssion)
			excludeValue.SQL = fmt.Sprintf("(%s IS NULL)", column.SortExpresssion)
		case prevValue != nil && column.NullOption == Last:
			// the next row could be NULL
			condition.SQL = fmt.Sprintf("((%s %s ?) OR (%s IS NULL))", column.SortExpresssion, sign, column.SortExpresssion)
			condition.Values = []interface{}{prevValue}
			excludeValue.SQL = fmt.Sprintf("(%s = ? AND %s IS NOT NULL)", column.SortExpresssion, column.SortExpresssion)
			excludeValue.Values = []interface{}{prevValue}
		case prevValue != nil && column.NullOption == First:
			condition.SQL = fmt.Sprintf("(%s %s ?)", column.SortExpresssion, sign)
			condition.Values = []interface{}{prevValue}
			excludeValue.SQL = fmt.Sprintf("(%s = ? AND %s IS NOT NULL)", column.SortExpresssion, column.SortExpresssion)
			excludeValue.Values = []interface{}{prevValue}
		}
		fmt.Printf("generated condition for column %v: %v\n", columns[0], condition)
		return condition, excludeValue
	} else {
		condition, excludeValue := generatePaginationForColumn(columns[1:], values[1:])
		var newCondition, newExcludeValue Condition
		switch {
		case prevValue == nil && column.NullOption == Last:
			newCondition.SQL = fmt.Sprintf("((%s IS NULL) AND %s)", column.SortExpresssion, condition.SQL)
			newExcludeValue.SQL = fmt.Sprintf("((%s IS NULL) AND %s)", column.SortExpresssion, excludeValue.SQL)
			newCondition.mergeValues(condition.Values)
			newExcludeValue.mergeValues(excludeValue.Values)
		case prevValue == nil && column.NullOption == First:
			newCondition.SQL = fmt.Sprintf("((%s IS NOT NULL) OR ((%s IS NULL) AND %s))", column.SortExpresssion, column.SortExpresssion, condition.SQL)
			newExcludeValue.SQL = fmt.Sprintf("((%s IS NULL) AND %s)", column.SortExpresssion, excludeValue.SQL)
			newCondition.mergeValues(condition.Values)
			newExcludeValue.mergeValues(excludeValue.Values)
		case prevValue != nil && column.NullOption == Last:
			// the next row could be NULL
			newCondition.SQL = fmt.Sprintf("((%s %s= ? OR %s IS NULL) AND (%s))", column.SortExpresssion, sign, column.SortExpresssion, condition.SQL)
			newCondition.mergeValues([]interface{}{prevValue})
			newCondition.mergeValues(condition.Values)

			newExcludeValue.SQL = fmt.Sprintf("((%s = ? AND %s IS NOT NULL) AND %s)", column.SortExpresssion, column.SortExpresssion, excludeValue.SQL)
			newExcludeValue.mergeValues([]interface{}{prevValue})
			newExcludeValue.mergeValues(excludeValue.Values)
		case prevValue != nil && column.NullOption == First:
			newCondition.SQL = fmt.Sprintf("(%s %s= ? AND (%s))", column.SortExpresssion, sign, condition.SQL)
			newCondition.mergeValues([]interface{}{prevValue})
			newCondition.mergeValues(condition.Values)
			newExcludeValue.SQL = fmt.Sprintf("((%s = ? AND %s IS NOT NULL) AND %s)", column.SortExpresssion, column.SortExpresssion, excludeValue.SQL)
			newExcludeValue.mergeValues([]interface{}{prevValue})
			newExcludeValue.mergeValues(excludeValue.Values)
		}
		fmt.Printf("generated condition for column %v: %v\n", columns[0], newCondition)
		return newCondition, newExcludeValue
	}
}
