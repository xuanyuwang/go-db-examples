package main

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

func main() {
	columnA := OrderByColumn{SortExpresssion: "A", Direction: Asc, NullOption: Last}
	condition := generatePaginationForColumn([]OrderByColumn{columnA}, []interface{}{20})
	fmt.Println(condition.SQL)
	fmt.Println(condition.Values)

	columnB := OrderByColumn{SortExpresssion: "B", Direction: Desc, NullOption: First}
	condition = generatePaginationForColumn([]OrderByColumn{columnA, columnB}, []interface{}{20, "2020-02-01"})
	fmt.Println(condition.SQL)
	fmt.Println(condition.Values)
}

func generatePaginationForColumn(
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
			condition.SQL = fmt.Sprintf("(%s IS NULL)", column.SortExpresssion)
		case prevValue == nil && column.NullOption != Last:
			condition.SQL = fmt.Sprintf("((%s IS NULL) OR (%s IS NOT NULL))", column.SortExpresssion, column.SortExpresssion)
		case prevValue != nil && column.NullOption == Last:
			// the next row could be NULL
			condition.SQL = fmt.Sprintf("((%s %s ?) OR (%s IS NULL))", column.SortExpresssion, sign, column.SortExpresssion)
			condition.Values = []interface{}{prevValue}
		case prevValue != nil && column.NullOption != Last:
			condition.SQL = fmt.Sprintf("(%s %s ?)", column.SortExpresssion, sign)
			condition.Values = []interface{}{prevValue}
		}
		return condition
	} else {
		condition := generatePaginationForColumn(columns[1:], values[1:])
		switch {
		case prevValue == nil && column.NullOption == Last:
			condition.SQL = fmt.Sprintf("((%s IS NULL) AND (%s))", column.SortExpresssion, condition.SQL)
		case prevValue == nil && column.NullOption != Last:
			condition.SQL = fmt.Sprintf("((%s IS NOT NULL) OR ((%s IS NULL) AND (%s)))", column.SortExpresssion, column.SortExpresssion, condition.SQL)
		case prevValue != nil && column.NullOption == Last:
			// the next row could be NULL
			condition.SQL = fmt.Sprintf("((%s %s ?) OR ((%s = ? OR %s IS NULL) AND (%s)))", column.SortExpresssion, sign, column.SortExpresssion, column.SortExpresssion, condition.SQL)
			condition.Values = append([]interface{}{prevValue, prevValue}, condition.Values)
		case prevValue != nil && column.NullOption != Last:
			condition.SQL = fmt.Sprintf("((%s %s ?) OR (%s = ? AND (%s)))", column.SortExpresssion, sign, column.SortExpresssion, condition.SQL)
			condition.Values = append([]interface{}{prevValue, prevValue}, condition.Values)
		}
		return condition
	}
}
