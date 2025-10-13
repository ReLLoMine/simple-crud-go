package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	// "log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type Response struct {
	body map[string]any
	code int
}

func newResponse() *Response {
	response := Response{
		body: make(map[string]any),
		code: -1,
	}
	return &response
}

type Environment struct {
	dbURI        string
	dbName       string
	dbCollection string
	dbUsername   string
	dbPassword   string
	serverHost   string
	serverPort   string
}

var env Environment = Environment{
	dbURI:        getEnv("DB_URI", "mongodb://127.0.0.1:27017"),
	dbName:       getEnv("DB_NAME", "simple_crud"),
	dbCollection: getEnv("DB_COLLECTION", "simple_crud"),
	dbUsername:   getEnv("DB_USERNAME", "admin"),
	dbPassword:   getEnv("DB_PASSWORD", "admin"),
	serverHost:   getEnv("SERVER_HOST", "0.0.0.0"),
	serverPort:   getEnv("SERVER_PORT", "8080"),
}

var client *mongo.Client
var mainContext context.Context
var collection *mongo.Collection

func makeResponse(err string, code int) (map[string]any, int) {
	res := make(map[string]any)
	res["message"] = err
	res["status"] = code
	return res, code
}

func jsonToMap(jsonString []byte) (map[string]any, error) {
	result := make(map[string]any)
	if err := json.Unmarshal(jsonString, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func readBody(req *http.Request) (map[string]any, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		log.Panic(err.Error())
	}
	if len(body) != 0 {
		return jsonToMap(body)
	}
	return nil, nil
}

func getEnv(envVar string, fallback string) string {
	if env := os.Getenv(envVar); env != "" {
		return env
	}
	return fallback
}

func getSR(path string) *mongo.SingleResult {
	ctx, cancel := context.WithTimeout(mainContext, time.Second*2)
	defer cancel()

	sr := collection.FindOne(ctx, bson.D{{Key: "path", Value: path}})
	if errors.Is(sr.Err(), mongo.ErrNoDocuments) {
		return nil
	} else if sr.Err() != nil {
		log.Panic(sr.Err().Error())
	}
	return sr
}

func getItem(path string) (map[string]any, int) {
	var i map[string]any
	if data := getSR(path); data == nil {
		i = make(map[string]any)
		return makeResponse("No item found", http.StatusNotFound)
	} else {
		if err := data.Decode(&i); err != nil {
			log.Panic(err.Error())
		}
		return i, http.StatusOK
	}
}

func createOrOverwriteItem(path string, data map[string]any) (map[string]any, int) {
	ctx, cancel := context.WithTimeout(mainContext, time.Second*2)
	defer cancel()

	data["path"] = path

	if getSR(path) == nil {
		if _, err := collection.InsertOne(ctx, data); err != nil {
			log.Panic(err.Error())
		}
	} else {
		if _, err := collection.ReplaceOne(ctx, bson.D{{Key: "path", Value: path}}, data); err != nil {
			log.Panic(err.Error())
		}
	}

	return makeResponse("Ok", http.StatusOK)
}

func createOrUpdateItem(path string, data map[string]any) (map[string]any, int) {
	ctx, cancel := context.WithTimeout(mainContext, time.Second*2)
	defer cancel()

	data["path"] = path

	if getSR(path) == nil {
		if _, err := collection.InsertOne(ctx, data); err != nil {
			log.Panic(err.Error())
		}
	} else {
		if _, err := collection.UpdateOne(ctx, bson.D{{Key: "path", Value: path}}, data); err != nil {
			return makeResponse(err.Error(), http.StatusBadRequest)
		}
	}

	return makeResponse("Ok", http.StatusOK)
}

func deleteItem(path string) (map[string]any, int) {
	ctx, cancel := context.WithTimeout(mainContext, time.Second*2)
	defer cancel()

	dr, err := collection.DeleteOne(ctx, bson.D{{Key: "path", Value: path}})
	if err != nil {
		log.Panic(err.Error())
	}

	return makeResponse(fmt.Sprintf("Deleted count: %d", dr.DeletedCount), http.StatusOK)
}

func mainHandler(rw http.ResponseWriter, req *http.Request) {
	response := newResponse()

	path := req.RequestURI
	body, err := readBody(req)
	log.Printf("Action %s to %s with: %s, err: %v", req.Method, path, body, err)

	if err != nil {
		response.body, response.code = makeResponse("Error reading JSON body", http.StatusBadRequest)
	} else {

		switch req.Method {

		case http.MethodGet:
			response.body, response.code = getItem(path)
		case http.MethodPost:
			response.body, response.code = createOrUpdateItem(path, body)
		case http.MethodPut:
			response.body, response.code = createOrOverwriteItem(path, body)
		case http.MethodDelete:
			response.body, response.code = deleteItem(path)
		default:
			response.body, response.code = makeResponse("Unsupported method", http.StatusBadRequest)
		}
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(response.code)
	json_bytes, err := json.Marshal(response.body)
	if err != nil {
		log.Panic(err.Error())
	}
	rw.Write(json_bytes)
}

func init() {
	// Init DB connection
	var err error
	client, err = mongo.Connect(options.Client().ApplyURI(env.dbURI))
	if err != nil {
		log.Fatal(err.Error())
	}

	// Create main context
	mainContext = context.Background()
}

func main() {
	ctx, cancel := context.WithTimeout(mainContext, time.Second*5)
	defer cancel()
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Panic(err.Error())
		}
	}()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		log.Panic(err.Error())
	}

	collection = client.Database(env.dbName).Collection(env.dbCollection)

	log.Printf("Runnging go server on %s:%s \n", env.serverHost, env.serverPort)
	log.Printf("Mongodb on %s use %s collection %s\n", env.dbURI, env.dbName, env.dbCollection)

	http.HandleFunc("/", mainHandler)

	if err := http.ListenAndServe(env.serverHost+":"+env.serverPort, nil); err != nil {
		log.Fatal(err.Error())
	}
}
