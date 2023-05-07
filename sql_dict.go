package sqldict

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	// _ "github.com/glebarez/go-sqlite"
	common "github.com/ilius/go-dict-commons"
	su "github.com/ilius/go-dict-commons/search_utils"
)

var ErrorHandler = func(err error) {
	log.Println(err)
}

const minScore = uint8(140)

// uriList[i] == "sqlite://PATH.db"
func Open(uriList []string, order map[string]int) []common.Dictionary {
	dicList := []common.Dictionary{}
	for _, uri := range uriList {
		i := strings.Index(uri, "://")
		if i < 1 {
			ErrorHandler(fmt.Errorf("invalid sql dict uri = %#v", uri))
			continue
		}
		driver := uri[:i]
		source := uri[i+3:]
		dic := NewDictionary(driver, source)
		name := dic.DictName()
		if order[name] < 0 {
			dic.disabled = true
		}
		dicList = append(dicList, dic)
	}
	return dicList
}

func NewDictionary(driver string, source string) *dictionaryImp {
	return &dictionaryImp{
		driver: driver,
		source: source,
	}
}

type dictionaryImp struct {
	disabled bool
	dictName string
	driver   string
	source   string
	hash     []byte

	db *sql.DB
}

func (d *dictionaryImp) Disabled() bool {
	return d.disabled
}

func (d *dictionaryImp) SetDisabled(disabled bool) {
	d.disabled = disabled
}

func (d *dictionaryImp) Loaded() bool {
	return d.db != nil
}

func (d *dictionaryImp) Load() error {
	db, err := sql.Open(d.driver, d.source)
	if err != nil {
		return err
	}
	d.db = db
	return nil
}

func (d *dictionaryImp) Close() {
	if d.db == nil {
		return
	}
	err := d.db.Close()
	if err != nil {
		log.Println(err)
	}
	d.db = nil
}

func (d *dictionaryImp) readInfo(key string) (string, error) {
	if d.db == nil {
		err := d.Load()
		if err != nil {
			return "", err
		}
	}
	row := d.db.QueryRow("SELECT value FROM meta WHERE key = ?", key)
	if row == nil {
		return "", fmt.Errorf("no %v in meta", key)
	}
	value := ""
	err := row.Scan(&value)
	if err != nil {
		return "", err
	}
	log.Printf("info: %v = %#v", key, value)
	return value, nil
}

func (d *dictionaryImp) DictName() string {
	if d.dictName != "" {
		return d.dictName
	}
	dictName, err := d.readInfo("name")
	if err != nil {
		log.Println(err)
		d.dictName = d.source
		return d.dictName
	}
	d.dictName = dictName
	return dictName
}

func (d *dictionaryImp) EntryCount() (int, error) {
	row := d.db.QueryRow("SELECT count(id) FROM entry")
	if row == nil {
		return 0, fmt.Errorf("EntryCount: row = nil")
	}
	count := 0
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *dictionaryImp) Description() string {
	desc, err := d.readInfo("description")
	if err != nil {
		log.Println(err)
		return ""
	}
	return desc
}

func (d *dictionaryImp) ResourceDir() string {
	// TODO
	return ""
}

func (d *dictionaryImp) ResourceURL() string {
	// TODO
	return ""
}

func (d *dictionaryImp) IndexPath() string {
	return ""
}

func (d *dictionaryImp) IndexFileSize() uint64 {
	return 0
}

func (d *dictionaryImp) InfoPath() string {
	return ""
}

func (d *dictionaryImp) CalcHash() ([]byte, error) {
	if d.hash != nil {
		return d.hash, nil
	}
	hexStr, err := d.readInfo("hash")
	if err != nil {
		return nil, err
	}
	hash := make([]byte, len(hexStr)/2)
	_, err = hex.Decode(hash, []byte(hexStr))
	if err != nil {
		return nil, err
	}
	d.hash = hash
	return hash, nil
}

func (d *dictionaryImp) readArticle(id int) []*common.SearchResultItem {
	row := d.db.QueryRow("SELECT article FROM entry WHERE id IS ?", id)
	if row == nil {
		log.Printf("No row with id = %v", id)
		return nil
	}
	article := ""
	err := row.Scan(&article)
	if err != nil {
		log.Println(err)
		return nil
	}
	return []*common.SearchResultItem{
		{
			Type: 'h',
			Data: []byte(article),
		},
	}
}

func (d *dictionaryImp) newResult(terms []string, id int, score uint8) *common.SearchResultLow {
	return &common.SearchResultLow{
		F_Score: score,
		F_Terms: terms,
		Items: func() []*common.SearchResultItem {
			return d.readArticle(id)
		},
		F_EntryIndex: uint64(id),
	}
}

func (d *dictionaryImp) getTerms(id int) ([]string, error) {
	row := d.db.QueryRow("SELECT term FROM entry WHERE id = ?", id)
	if row == nil {
		return nil, fmt.Errorf("id %v was not found", id)
	}
	head := ""
	err := row.Scan(&head)
	if err != nil {
		return nil, err
	}
	terms := []string{head}
	term := ""
	rows, err := d.db.Query("SELECT term FROM alt WHERE id = ?", id)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(&term)
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
	}
	return terms, nil
}

func (d *dictionaryImp) EntryByIndex(index int) *common.SearchResultLow {
	terms, err := d.getTerms(index + 1)
	if err != nil {
		log.Println(err)
		return nil
	}
	return d.newResult(terms, index, 0)
}

func (d *dictionaryImp) SearchFuzzy(query string, _ int, _ time.Duration) []*common.SearchResultLow {
	if len(query) < 2 {
		log.Println("SQLite fuzzy search does not support query smaller than 2 letters")
		return nil
	}

	t0 := time.Now()
	query = strings.ToLower(strings.TrimSpace(query))
	queryWords := strings.Split(query, " ")
	queryRunes := []rune(query)
	maxSubCount := len(queryRunes) - 2*len(queryWords)
	if maxSubCount < 0 {
		// for example query is: "a", "a b", "a b c"
		return d.SearchStartWith(query, 0, 0)
	}
	sqlArgs := make([]any, 0, maxSubCount)
	subMap := make(map[string]bool, maxSubCount)
	for _, word := range queryWords {
		e_runes := []rune("\n" + word)
		n := len(e_runes) - 2
		for i := 0; i < n; i++ {
			sub := string(e_runes[i : i+3])
			if subMap[sub] {
				continue
			}
			subMap[sub] = true
			sqlArgs = append(sqlArgs, sub)
		}
	}
	qMarks := strings.Repeat("?, ", len(sqlArgs)-1) + "?"
	sqlQuery := "SELECT DISTINCT id, term FROM fuzzy3 WHERE sub IN(" + qMarks + ");"
	log.Printf("%#v  args: %#v", sqlQuery, sqlArgs)
	rows, err := d.db.Query(sqlQuery, sqlArgs...)
	if err != nil {
		ErrorHandler(err)
		return nil
	}

	log.Printf("SearchFuzzy SQL query took %v for %#v on %s\n", time.Since(t0), query, d.DictName())
	t1 := time.Now()

	mainWordIndex := 0
	for mainWordIndex < len(queryWords)-1 && queryWords[mainWordIndex] == "*" {
		mainWordIndex++
	}

	minWordCount := 1
	queryWordCount := 0
	for _, word := range queryWords {
		if word == "*" {
			minWordCount++
			continue
		}
		queryWordCount++
	}

	args := &su.ScoreFuzzyArgs{
		Query:          query,
		QueryRunes:     queryRunes,
		QueryMainWord:  []rune(queryWords[mainWordIndex]),
		QueryWordCount: queryWordCount,
		MinWordCount:   minWordCount,
		MainWordIndex:  mainWordIndex,
	}
	id := -1
	term := ""
	scoreMap := map[int]uint8{}
	buff := make([]uint16, 500)
	for rows.Next() {
		err := rows.Scan(&id, &term)
		if err != nil {
			ErrorHandler(err)
			return nil
		}
		score := su.ScoreFuzzySingle(term, args, buff)
		if score < minScore {
			continue
		}
		if score > scoreMap[id] {
			scoreMap[id] = score
		}
	}
	log.Printf("SearchFuzzy query loop took %v for %#v on %s\n", time.Since(t1), query, d.DictName())
	t2 := time.Now()
	results := []*common.SearchResultLow{}
	for id, score := range scoreMap {
		terms, err := d.getTerms(id)
		if err != nil {
			ErrorHandler(err)
			return nil
		}
		results = append(results, d.newResult(terms, id, score))
	}

	log.Printf("SearchFuzzy score loop took %v for %#v on %s", time.Since(t2), query, d.DictName())
	return results
}

func (d *dictionaryImp) searchDB(termCond string, arg string) *sql.Rows {
	sqlQ := "SELECT entry.id, entry.term, " +
		"json_group_array(alt.term)" +
		"FROM entry LEFT JOIN alt ON entry.id=alt.id " +
		"WHERE entry.term " + termCond + " " +
		"OR alt.term " + termCond + " " +
		"GROUP BY entry.id;"
	rows, err := d.db.Query(
		sqlQ,
		arg,
		arg,
	)
	if err != nil {
		ErrorHandler(fmt.Errorf("error running SQL query %#v: %v", sqlQ, err))
		return nil
	}
	return rows
}

func (d *dictionaryImp) SearchStartWith(query string, _ int, _ time.Duration) []*common.SearchResultLow {
	t1 := time.Now()
	query = strings.ToLower(strings.TrimSpace(query))
	rows := d.searchDB("LIKE ?", query+"%")
	if rows == nil {
		return nil
	}
	results := []*common.SearchResultLow{}
	for rows.Next() {
		id := -1
		term := ""
		altsJson := ""
		err := rows.Scan(&id, &term, &altsJson)
		if err != nil {
			ErrorHandler(err)
			return nil
		}
		var alts []string
		err = json.Unmarshal([]byte(altsJson), &alts)
		if err != nil {
			ErrorHandler(err)
		}
		terms := append([]string{term}, alts...)
		score := su.ScoreStartsWith(terms, query)
		if score < minScore {
			continue
		}
		results = append(results, d.newResult(terms, id, score))
	}
	dt := time.Since(t1)
	if dt > time.Millisecond {
		log.Printf("SearchStartWith index loop took %v for %#v on %s\n", dt, query, d.DictName())
	}
	return results
}

func (d *dictionaryImp) searchPattern(
	termCond string,
	arg string,
	checkTerm func(string) uint8,
) []*common.SearchResultLow {
	rows := d.searchDB(termCond, arg)
	if rows == nil {
		return nil
	}
	results := []*common.SearchResultLow{}
	for rows.Next() {
		id := -1
		term := ""
		altsJson := ""
		err := rows.Scan(&id, &term, &altsJson)
		if err != nil {
			ErrorHandler(err)
			return nil
		}
		var alts []string
		err = json.Unmarshal([]byte(altsJson), &alts)
		if err != nil {
			ErrorHandler(err)
		}
		terms := append([]string{term}, alts...)
		score := uint8(0)
		for _, term := range terms {
			termScore := checkTerm(term)
			if termScore > score {
				score = termScore
				break
			}
		}
		if score < minScore {
			continue
		}
		results = append(results, d.newResult(terms, id, score))
	}
	return results
}

func (d *dictionaryImp) SearchRegex(query string, _ int, _ time.Duration) ([]*common.SearchResultLow, error) {
	t1 := time.Now()
	results := d.searchPattern("REGEXP ?", "^"+query+"$", func(term string) uint8 {
		if len(term) < 20 {
			return 200 - uint8(len(term))
		}
		return 180
	})
	dt := time.Since(t1)
	if dt > time.Millisecond {
		log.Printf("SearchRegex index loop took %v for %#v on %s\n", dt, query, d.DictName())
	}
	return results, nil
}

func (d *dictionaryImp) SearchGlob(query string, _ int, _ time.Duration) ([]*common.SearchResultLow, error) {
	t1 := time.Now()
	results := d.searchPattern("GLOB ?", query, func(term string) uint8 {
		if len(term) < 20 {
			return 200 - uint8(len(term))
		}
		return 180
	})
	dt := time.Since(t1)
	if dt > time.Millisecond {
		log.Printf("SearchGlob index loop took %v for %#v on %s\n", dt, query, d.DictName())
	}
	return results, nil
}
