package drpc

type DBResult struct {
	answer *Answer
}

func NewDBResult(answer *Answer) *DBResult {

	dbResult := &DBResult{
		answer: answer,
	}

	return dbResult
}

func (dbResult *DBResult) IsError() bool {
	return dbResult.answer.IsException()
}

func (dbResult *DBResult) GetErrorCode() int {
	code, ok := dbResult.answer.GetInt("code")
	if !ok {
		return -1
	}
	return code
}

func (dbResult *DBResult) GetErrorMessage() string {
	ex, ok := dbResult.answer.GetString("ex")
	if !ok {
		return ""
	}
	return ex
}

func (dbResult *DBResult) GetAffectedRows() (int64, bool) {
	affectedRows, ok := dbResult.answer.GetInt64("affectedRows")
	if !ok {
		return 0, false
	}
	return affectedRows, true
}

func (dbResult *DBResult) GetInsertId() (int64, bool) {
	insertId, ok := dbResult.answer.GetInt64("insertId")
	if !ok {
		return 0, false
	}
	return insertId, true
}

func (dbResult *DBResult) GetRowsCount() (int64, bool) {
	rows, ok := dbResult.answer.GetSlice("rows")
	if !ok {
		return 0, false
	}
	return int64(len(rows)), true
}

func (dbResult *DBResult) GetFields() ([]string, bool) {
	fieldsSlice, ok := dbResult.answer.GetSlice("fields")
	if !ok {
		return []string{}, false
	}
	fields := make([]string, 0)
	for _, v := range fieldsSlice {
		fields = append(fields, v.(string))
	}
	return fields, true
}

func (dbResult *DBResult) GetIndexRow(index int) (map[string]interface{}, bool) {
	rows, ok := dbResult.answer.GetSlice("rows")
	if !ok {
		return nil, false
	}
	if len(rows) == 0 {
		return make(map[string]interface{}), true
	}
	fields, ok := dbResult.GetFields()
	if !ok {
		return nil, false
	}

	row := rows[index]
	rowList := row.([]interface{})

	if len(fields) != len(rowList) {
		return nil, false
	}

	rowMap := make(map[string]interface{})
	for i := 0; i < len(fields); i++ {
		rowMap[fields[i]] = rowList[i]
	}

	return rowMap, true
}

func (dbResult *DBResult) GetSingleRow() (map[string]interface{}, bool) {
	return dbResult.GetIndexRow(0)
}

func (dbResult *DBResult) GetRows() ([]map[string]interface{}, bool) {
	rows := make([]map[string]interface{}, 0)
	rowCount, ok := dbResult.GetRowsCount()
	if !ok {
		return rows, false
	}

	for i := 0; i < int(rowCount); i++ {
		row, ok := dbResult.GetIndexRow(i)
		if !ok {
			return rows, false
		}
		rows = append(rows, row)
	}

	return rows, true
}
