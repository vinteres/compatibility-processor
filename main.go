package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v4"
)

type userAnswer struct {
	user_id     string
	answer_id   string
	question_id string
}

type userInterest struct {
	id         string
	created_at int
}

type userCompatability struct {
	userOneId  string
	userTwoId  string
	percentage int
}

func homePage(w http.ResponseWriter, r *http.Request) {
	userIds, ok := r.URL.Query()["userId"]

	if !ok || len(userIds) == 0 {
		log.Println("Url Param 'userId' is missing")
		return
	}
	userId := userIds[0]

	calculateCompatibility(userId)
}

func handleRequests() {
	http.HandleFunc("/calculate-compatibility", homePage)
	log.Fatal(http.ListenAndServe(":10000", nil))
}

func main() {
	handleRequests()
}

func calculateCompatibility(userId string) {
	var err error
	var conn *pgx.Conn
	ctx := context.Background()
	conn, err = pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)

		return
	}

	var compatibilities = getCompatibility(userId, conn)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return
	}

	err = setCompatibility(compatibilities, conn)
	if err != nil {
		tx.Rollback(ctx)

		return
	}

	tx.Commit(ctx)
}

func setCompatibility(compatibilities []userCompatability, conn *pgx.Conn) error {
	if len(compatibilities) == 0 {
		return nil
	}

	var chunks []([]userCompatability)
	chunkSize := 10
	for i := 0; i < len(compatibilities); i += chunkSize {
		start := i
		end := start + chunkSize
		if end >= len(compatibilities) {
			end = len(compatibilities)
		}
		var rc []userCompatability
		for start < end {
			rc = append(rc, compatibilities[start])
			start++
		}

		chunks = append(chunks, rc)
	}

	var rx = 0
	for _, chunk := range chunks {
		rx += len(chunk)
		var values []string = make([]string, len(chunk))
		for ci, item := range chunk {
			values[ci] = fmt.Sprintf("('%s', '%s', %d)", item.userOneId, item.userTwoId, item.percentage)
		}
		query := fmt.Sprintf("INSERT INTO user_compatibilities (user_one_id, user_two_id, percent) VALUES %s", strings.Join(values[:], ","))

		_, err := conn.Exec(context.Background(), query)
		if err != nil {
			return err
		}
	}

	return nil
}

func getCompatibility(userId string, conn *pgx.Conn) []userCompatability {
	var foundMatches int = 0
	var lastCreatedAt int = -1
	var compatibilities []userCompatability

	gender, interested_in, err := getUserInfoById(userId, conn)
	if err != nil {
		return []userCompatability{}
	}

	var userOneAnswers map[string]string
	userOneAnswers, err = findAllAnswersForUser(userId, conn)
	if err != nil {
		return []userCompatability{}
	}

	for {
		var users []userInterest
		users, err = findInterestedIds(gender, interested_in, lastCreatedAt, conn)
		if err != nil {
			return []userCompatability{}
		}
		if 0 >= len(users) {
			break
		}

		var userIds []string = make([]string, len(users))
		for i, e := range users {
			userIds[i] = e.id
		}

		var usersAnswers []userAnswer
		usersAnswers, err = findAllAnswersForUsers(userIds, conn)
		if err != nil {
			return []userCompatability{}
		}

		for _, iUserId := range userIds {
			var userAnswers map[string]string = make(map[string]string)
			for _, ua := range usersAnswers {
				if ua.user_id != iUserId {
					continue
				}

				userAnswers[ua.question_id] = ua.answer_id
			}

			var percentMatch int = getCompatibilityPercentBetween(userOneAnswers, userAnswers)
			if percentMatch >= 40 {
				foundMatches++
				compatibilities = append(compatibilities, userCompatability{userOneId: userId, userTwoId: iUserId, percentage: percentMatch})

				if foundMatches >= 100 {
					return compatibilities
				}
			}
		}

		lastCreatedAt = users[len(users)-1].created_at
	}

	return compatibilities
}

func getCompatibilityPercentBetween(userOneAnswers map[string]string, userTwoAnswers map[string]string) int {
	var c int = 0
	for key, answerId := range userOneAnswers {
		if answerId == userTwoAnswers[key] {
			c++
		}
	}

	var percentMatch int = 0
	if c == 0 {
		percentMatch = 0
	} else {
		percentMatch = int(math.Ceil(percentage(c, len(userOneAnswers))))
	}
	if percentMatch > 100 {
		percentMatch = 100
	}

	return percentMatch
}

func percentage(count int, total int) float64 {
	return float64(100 * count / total)
}

func findAllAnswersForUsers(userIds []string, conn *pgx.Conn) ([]userAnswer, error) {
	var err error
	var result []userAnswer
	var pIds []string = make([]string, len(userIds))

	for i, e := range userIds {
		pIds[i] = fmt.Sprintf("'%s'", e)
	}

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM user_answers WHERE user_id IN (%s)", strings.Join(pIds[:], ",")))
	if err != nil {
		return []userAnswer{}, err
	}

	for rows.Next() {
		var i userAnswer
		err := rows.Scan(&i.user_id, &i.answer_id, &i.question_id)
		if err != nil {
			return []userAnswer{}, err
		}

		result = append(result, i)
	}

	return result, nil
}

func findInterestedIds(gender string, interested_in string, createdAt int, conn *pgx.Conn) ([]userInterest, error) {
	var whereCreatedAt string = ""
	if createdAt > 0 {
		whereCreatedAt = fmt.Sprintf("AND created_at < %d", createdAt)
	}

	var err error
	var result []userInterest

	rows, err := conn.Query(
		context.Background(),
		fmt.Sprintf(
			`SELECT id, created_at FROM users
      WHERE interested_in = '%s' AND gender = '%s' %s
      ORDER BY created_at DESC
      LIMIT 100`, gender, interested_in, whereCreatedAt))

	if err != nil {
		return []userInterest{}, err
	}

	for rows.Next() {
		var i userInterest
		err := rows.Scan(&i.id, &i.created_at)
		if err != nil {
			return []userInterest{}, err
		}

		result = append(result, i)
	}

	return result, nil
}

func findAllAnswersForUser(userId string, conn *pgx.Conn) (map[string]string, error) {
	var err error
	var result map[string]string = make(map[string]string)

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM user_answers WHERE user_id = '%s'", userId))
	if err != nil {
		return map[string]string{}, err
	}

	for rows.Next() {
		var i userAnswer
		err := rows.Scan(&i.user_id, &i.answer_id, &i.question_id)
		if err != nil {
			return map[string]string{}, err
		}

		result[i.question_id] = i.answer_id
	}

	return result, nil
}

func getUserInfoById(userId string, conn *pgx.Conn) (string, string, error) {
	var err error
	var gender string
	var interested_in string

	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT gender, interested_in FROM users WHERE id = '%s'", userId)).Scan(&gender, &interested_in)
	if err != nil {
		return "", "", err
	}

	return gender, interested_in, nil
}
