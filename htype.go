/*


*/

package goh

import (
	"fmt"
	"github.com/jinntrance/goh/Hbase"
)

//type Text []byte

func textListToStr(list []Hbase.Text) []string {
	if list == nil {
		return nil
	}

	l := len(list)
	data := make([]string, l)
	for i := 0; i < l; i++ {
		data[i] = string(list[i])
	}

	return data
}

func toHbaseTextList(list []string) []Hbase.Text {
	if list == nil {
		return nil
	}

	l := len(list)
	data := make([]Hbase.Text, l)
	for i := 0; i < l; i++ {
		data[i] = Hbase.Text(list[i])
	}
	return data
}

func toHbaseTextListFromByte(list [][]byte) []Hbase.Text {
	if list == nil {
		return nil
	}

	l := len(list)
	data := make([]Hbase.Text, l)
	for i := 0; i < l; i++ {
		data[i] = Hbase.Text(list[i])
	}
	return data
}

func toHbaseTextMap(source map[string]string) map[Hbase.Text]Hbase.Text {
	if source == nil {
		return nil
	}

	data := make(map[Hbase.Text]Hbase.Text, len(source))
	for k, v := range source {
		data[Hbase.Text(k)] = Hbase.Text(v)
	}

	return data
}

// /**
//  * A Mutation object is used to either update or delete a column-value.
//  * 
//  * Attributes:
//  *  - IsDelete
//  *  - Column
//  *  - Value
//  *  - WriteToWAL
//  */
// type Mutation struct {
// 	IsDelete   bool   "isDelete"   // 1
// 	Column     []byte "column"     // 2
// 	Value      []byte "value"      // 3
// 	WriteToWAL bool   "writeToWAL" // 4
// }

func NewMutation(column string, value []byte) *Hbase.Mutation {
	return &Hbase.Mutation{
		IsDelete:   false,
		WriteToWAL: true,
		Column:     Hbase.Text(column),
		Value:      Hbase.Text(value),
	}

}

// /**
//  * A BatchMutation object is used to apply a number of Mutations to a single row.
//  * 
//  * Attributes:
//  *  - Row
//  *  - Mutations
//  */
// type BatchMutation struct {
// 	Row       []byte      "row"       // 1
// 	Mutations []*Mutation "mutations" // 2
// }

func NewBatchMutation(row []byte, mutations []*Hbase.Mutation) *Hbase.BatchMutation {
	return &Hbase.BatchMutation{
		Row:       Hbase.Text(row),
		Mutations: mutations,
	}

}

// /**
//  * For increments that are not incrementColumnValue
//  * equivalents.
//  * 
//  * Attributes:
//  *  - Table
//  *  - Row
//  *  - Column
//  *  - Ammount
//  */
// type TIncrement struct {
// 	Table   []byte "table"   // 1
// 	Row     []byte "row"     // 2
// 	Column  []byte "column"  // 3
// 	Ammount int64  "ammount" // 4
// }

func NewTIncrement(table string, row []byte, column string, ammount int64) *Hbase.TIncrement {
	return &Hbase.TIncrement{
		Table:   Hbase.Text(table),
		Row:     Hbase.Text(row),
		Column:  Hbase.Text(column),
		Ammount: ammount,
	}
}

/**
 * Holds row name and then a map of columns to cells.
 * 
 * Attributes:
 *  - Row
 *  - Columns
 */
// type TRowResult struct {
// 	Row     string            "row"     // 1
// 	Columns map[string]*TCell "columns" // 2
// }

/**
 * A Scan object is used to specify scanner parameters when opening a scanner.
 * 
 * Attributes:
 *  - StartRow
 *  - StopRow
 *  - Timestamp
 *  - Columns
 *  - Caching
 *  - FilterString
 */
type TScan struct {
	StartRow     []byte   "startRow"     // 1
	StopRow      []byte   "stopRow"      // 2
	Timestamp    int64    "timestamp"    // 3
	Columns      []string "columns"      // 4
	Caching      int32    "caching"      // 5
	FilterString string   "filterString" // 6
}

func toHbaseTScan(scan *TScan) *Hbase.TScan {
	if scan == nil {
		return nil
	}

	if scan.FilterString == "" {
		return &Hbase.TScan{
			StartRow:     Hbase.Text(scan.StartRow),
			StopRow:      Hbase.Text(scan.StopRow),
			Timestamp:    scan.Timestamp,
			Columns:      toHbaseTextList(scan.Columns),
			Caching:      scan.Caching,
			FilterString: "",
		}
	}

	return &Hbase.TScan{
		StartRow:     Hbase.Text(scan.StartRow),
		StopRow:      Hbase.Text(scan.StopRow),
		Timestamp:    scan.Timestamp,
		Columns:      toHbaseTextList(scan.Columns),
		Caching:      scan.Caching,
		FilterString: Hbase.Text(scan.FilterString),
	}

}

// /**
//  * TCell - Used to transport a cell value (byte[]) and the timestamp it was
//  * stored with together as a result for get and getRow methods. This promotes
//  * the timestamp of a cell to a first-class value, making it easy to take
//  * note of temporal data. Cell is used all the way from HStore up to HTable.
//  * 
//  * Attributes:
//  *  - Value
//  *  - Timestamp
//  */
// type TCell struct {
// 	Value     []byte "value"     // 1
// 	Timestamp int64  "timestamp" // 2
// }

// /**
//  * An IOError exception signals that an error occurred communicating
//  * to the Hbase master or an Hbase region server.  Also used to return
//  * more general Hbase error conditions.
//  * 
//  * Attributes:
//  *  - Message
//  */
// type IOError struct {
// 	Message string "message" // 1
// }

// /**
//  * An IllegalArgument exception indicates an illegal or invalid
//  * argument was passed into a procedure.
//  * 
//  * Attributes:
//  *  - Message
//  */
// type IllegalArgument struct {
// 	Message string "message" // 1
// }

// /**
//  * An AlreadyExists exceptions signals that a table with the specified
//  * name already exists
//  * 
//  * Attributes:
//  *  - Message
//  */
// type AlreadyExists struct {
// 	Message string "message" // 1
// }

// func (t *AlreadyExists) String() string {
// 	if t == nil {
// 		return "<nil>"
// 	}
// 	return t.Message
// }

func name() {
	fmt.Println("...")
}
