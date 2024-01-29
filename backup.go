package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	minio "github.com/minio/minio-go"
	yaml "gopkg.in/yaml.v2"
	inotify "k8s.io/utils/inotify"
)

// Global variables are fun :) - this is a channel that we can send a message to if things go pear shaped
var (
	health = make(chan bool)
)

type Config struct {
	S3 struct {
		AWS_ACCESS_KEY_ID     string `yaml:"aws_access_key_id"`
		AWS_SECRET_ACCESS_KEY string `yaml:"aws_secret_access_key"`
		AWS_REGION            string `yaml:"aws_region"`
		AWS_BUCKET            string `yaml:"aws_bucket"`
		AWS_URL               string `yaml:"aws_url"`
		AWS_S3_TLS_INSECURE   bool   `yaml:"aws_s3_tls_insecure"`
	} `yaml:"S3"`
	MYSQL struct {
		MARIADB_HOST         string   `yaml:"mariadb_host"`
		MARIADB_PORT         string   `yaml:"mariadb_port"`
		MARIADB_USER         string   `yaml:"mariadb_user"`
		MARIADB_PASSWORD     string   `yaml:"mariadb_password"`
		MARIADB_DATABASE     string   `yaml:"mariadb_database"`
		MYSQLDUMP_EXTRA_ARGS []string `yaml:"mysqldump_extra_args"`
	} `yaml:"MYSQL"`
}

// New config returns a new config struct with default values
func NewConfig() *Config {
	return &Config{
		S3: struct {
			AWS_ACCESS_KEY_ID     string `yaml:"aws_access_key_id"`
			AWS_SECRET_ACCESS_KEY string `yaml:"aws_secret_access_key"`
			AWS_REGION            string `yaml:"aws_region"`
			AWS_BUCKET            string `yaml:"aws_bucket"`
			AWS_URL               string `yaml:"aws_url"`
			AWS_S3_TLS_INSECURE   bool   `yaml:"aws_s3_tls_insecure"`
		}{
			AWS_ACCESS_KEY_ID:     "",
			AWS_SECRET_ACCESS_KEY: "",
			AWS_REGION:            "",
			AWS_BUCKET:            "",
			AWS_URL:               "",
			AWS_S3_TLS_INSECURE:   false,
		},
		MYSQL: struct {
			MARIADB_HOST         string   `yaml:"mariadb_host"`
			MARIADB_PORT         string   `yaml:"mariadb_port"`
			MARIADB_USER         string   `yaml:"mariadb_user"`
			MARIADB_PASSWORD     string   `yaml:"mariadb_password"`
			MARIADB_DATABASE     string   `yaml:"mariadb_database"`
			MYSQLDUMP_EXTRA_ARGS []string `yaml:"mysqldump_extra_args"`
		}{
			MARIADB_HOST:         "",
			MARIADB_PORT:         "",
			MARIADB_USER:         "",
			MARIADB_PASSWORD:     "",
			MARIADB_DATABASE:     "",
			MYSQLDUMP_EXTRA_ARGS: []string{},
		},
	}
}

// Runs mysqldump and uploads the result to S3
func (c *Config) Backup(path string) error {
	/* As our config can change, we do this every time we run:
	- Check that our bucket exists, if not create it
	- Check that mysqldump exists
	- Setup the minioclient
	*/
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Fatalln("mysqldump binary not found")
	}
	// S3 shenanigans
	minioClient, err := minio.New(c.S3.AWS_URL, c.S3.AWS_ACCESS_KEY_ID, c.S3.AWS_SECRET_ACCESS_KEY, c.S3.AWS_S3_TLS_INSECURE)
	if err != nil {
		log.Println(err)
		return err
	}
	minioClient.SetCustomTransport(&http.Transport{
		IdleConnTimeout:    120 * time.Second,
		DisableCompression: true,
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: c.S3.AWS_S3_TLS_INSECURE},
	})
	// Create the bucket if it doesn't exist
	exists, err := minioClient.BucketExists(c.S3.AWS_BUCKET)
	if err != nil {
		log.Println(err)
		return err
	}
	if !exists {
		log.Printf("Bucket %s does not exist, creating\n", c.S3.AWS_BUCKET)
		err = minioClient.MakeBucket(c.S3.AWS_BUCKET, c.S3.AWS_REGION)
		if err != nil {
			log.Println("Error creating bucket")
			log.Fatalln(err)
		} else {
			log.Printf("Successfully created bucket %s\n", c.S3.AWS_BUCKET)
		}
	} else {
		log.Printf("Successfully found bucket %s\n", c.S3.AWS_BUCKET)
	}

	/* This is where we actually do the backup:
	- Sets up the mysqldump command
	- Runs it in a shell
	- Uploads the file to S3
	- Cleans up
	*/
	runTime := string(time.Now().Format(time.RFC3339))
	fileName := "/tmp/" + runTime + ".sql"
	var mysqldumpArgs string
	for _, arg := range c.MYSQL.MYSQLDUMP_EXTRA_ARGS {
		mysqldumpArgs = mysqldumpArgs + " " + arg
	}
	if c.MYSQL.MARIADB_DATABASE != "" {
		os.Setenv("MYSQL_DATABASE", c.MYSQL.MARIADB_DATABASE)
	} else {
		mysqldumpArgs = mysqldumpArgs + " --all-databases"
	}
	_, err = os.Create(fileName)
	if err != nil {
		log.Println("Error creating file")
		log.Fatalln(err)
	}
	commandString := fmt.Sprintf("%s -h %s -P %s -u %s --password=%s --result-file=%s %s %s", path, c.MYSQL.MARIADB_HOST, c.MYSQL.MARIADB_PORT, c.MYSQL.MARIADB_USER, c.MYSQL.MARIADB_PASSWORD, fileName, c.MYSQL.MARIADB_DATABASE, mysqldumpArgs)
	//Call it in a shell so we can use the env vars
	cmd, err := exec.Command("sh", "-c", commandString).Output()
	if err != nil {
		log.Fatalln("Error running mysqldump:", cmd, err)
		health <- false
	} else {
		log.Println("mysqldump complete")
	}

	// Upload the file
	_, err = minioClient.FPutObject(c.S3.AWS_BUCKET, runTime, fileName, minio.PutObjectOptions{ContentType: "application/sql"})
	if err != nil {
		log.Println("Error uploading file to S3")
		log.Println(err)
		health <- false
	} else {
		log.Println("Successfully uploaded file to S3")
	}
	// Cleanup
	err = os.Remove(fileName)
	if err != nil {
		log.Println("Error removing file", fileName, "-", err)
	}
	log.Printf("Backup complete. Uploaded to %s at %s", c.S3.AWS_BUCKET, time.Now().Format(time.RFC3339))
	health <- true
	return nil
}

// Sketchy as heck - listens on a port for health checks that it reads from a channel
func healthz(port string) {
	http.ListenAndServe(":"+port, nil)
	for {
		select {
		case <-health:
			http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.WriteHeader(http.StatusOK)
				}
			})
		default:
			http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.WriteHeader(http.StatusInternalServerError)
				}
			})
		}
	}
}

// Uses inotify to watch the config file for changes, if it changes then we send a message to the channel to reload
func (c *Config) reloadWatcher(configFile string) {
	watcher, err := inotify.NewWatcher()
	if err != nil {
		log.Println(err)
		log.Println("Cannot watch config file for changes")
		health <- false
	}
	err = watcher.Watch(configFile)
	if err != nil {
		log.Println(err)
		log.Println("Error watching config file for changes")
		health <- false
	} else {
		log.Println("Watching config file for changes")
	}
	for {
		select {
		case ev := <-watcher.Event:
			if ev.Mask == inotify.InModify {
				log.Println("Config file changed, reloading")
				c.ReadConfig(configFile)
			}
		case err := <-watcher.Error:
			log.Println(err)
			log.Println("Error watching config file for changes")
			health <- false
		}
	}
}

// Reads the config file and unmarshals it into the config struct
func (c *Config) ReadConfig(configFile string) error {
	file, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalln(err)
	}
	err = yaml.Unmarshal(file, c)
	if err != nil {
		log.Fatalln(err)
	}
	return nil
}

func main() {
	var (
		env           = flag.Bool("env", false, "Use environment variables instead of config file")
		configFile    = flag.String("config", "/etc/mysql-s3/config.yaml", "Path to config file")
		duration      = flag.Duration("duration", 24*time.Hour, "Duration between backups")
		version       = flag.Bool("v", false, "Print version and exit")
		mysqldump     = flag.String("mysqldump", "/usr/bin/mysqldump", "Path to mysqldump binary")
		healthPort    = flag.String("port", "8090", "Address to listen on for health checks")
		healthEnabled = flag.Bool("health", true, "Enable health checks")
		curVersion    = string("0.1.0")
	)
	flag.Parse()
	if *version {
		fmt.Println(curVersion)
		os.Exit(0)
	}
	log.Println("Starting mysql-s3 backup version", curVersion)
	log.Println("Schedule set for every", *duration)

	var config = NewConfig()
	if *env {
		log.Println("Using environment variables for config")
	} else {
		log.Println("Using config file", *configFile)
		config.ReadConfig(*configFile)
	}

	// Setup our little watchers
	if *healthEnabled {
		go healthz(*healthPort)
	}
	go config.reloadWatcher(*configFile)

	for {
		config.Backup(*mysqldump)
		time.Sleep(*duration)
	}

}
