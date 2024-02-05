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

type Config struct {
	Backups []struct {
		Name string `yaml:"name"`
		S3   struct {
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
	} `yaml:"backups"`
}

// New config returns a new config struct with default values
func NewConfig() *Config {
	return &Config{}
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
	for _, backup := range c.Backups {
		log.Println("Starting backup of", backup.Name)
		minioClient, err := minio.New(backup.S3.AWS_URL, backup.S3.AWS_ACCESS_KEY_ID, backup.S3.AWS_SECRET_ACCESS_KEY, backup.S3.AWS_S3_TLS_INSECURE)
		if err != nil {
			log.Println(err)
			return err
		}
		minioClient.SetCustomTransport(&http.Transport{
			IdleConnTimeout:    120 * time.Second,
			DisableCompression: true,
			TLSClientConfig:    &tls.Config{InsecureSkipVerify: backup.S3.AWS_S3_TLS_INSECURE},
		})
		// Create the bucket if it doesn't exist
		exists, err := minioClient.BucketExists(backup.S3.AWS_BUCKET)
		if err != nil {
			log.Println(err)
			return err
		}
		if !exists {
			log.Printf("Bucket %s does not exist, creating\n", backup.S3.AWS_BUCKET)
			err = minioClient.MakeBucket(backup.S3.AWS_BUCKET, backup.S3.AWS_REGION)
			if err != nil {
				log.Println("Error creating bucket", backup.S3.AWS_BUCKET, err)
				continue
			} else {
				log.Printf("Successfully created bucket %s\n", backup.S3.AWS_BUCKET)
			}
		} else {
			log.Printf("Successfully found bucket %s\n", backup.S3.AWS_BUCKET)
		}

		/* This is where we actually do the backup:
		- Sets up the mysqldump command
		- Runs it in a shell
		- Uploads the file to S3
		- Cleans up
		*/
		log.Println("Running mysqldump")
		runTime := string(time.Now().Format(time.RFC3339))
		fileName := "/tmp/" + runTime + "-" + backup.Name + ".sql"
		var mysqldumpArgs string
		for _, arg := range backup.MYSQL.MYSQLDUMP_EXTRA_ARGS {
			mysqldumpArgs = mysqldumpArgs + " " + arg
		}
		if backup.MYSQL.MARIADB_DATABASE != "" {
			os.Setenv("MYSQL_DATABASE", backup.MYSQL.MARIADB_DATABASE)
		} else {
			mysqldumpArgs = mysqldumpArgs + " --all-databases"
		}
		_, err = os.Create(fileName)
		if err != nil {
			log.Println("Error creating file", fileName, "-", err)
			continue
		}
		commandString := fmt.Sprintf("%s -h %s -P %s -u %s --password=%s --result-file=%s %s %s", path, backup.MYSQL.MARIADB_HOST, backup.MYSQL.MARIADB_PORT, backup.MYSQL.MARIADB_USER, backup.MYSQL.MARIADB_PASSWORD, fileName, backup.MYSQL.MARIADB_DATABASE, mysqldumpArgs)
		//Call it in a shell so we can use the env vars
		cmd, err := exec.Command("sh", "-c", commandString).Output()
		if err != nil {
			log.Println("Error running mysqldump:", (string(cmd)), err)
			continue
		} else {
			log.Println("mysqldump complete")
		}

		// Upload the file
		_, err = minioClient.FPutObject(backup.S3.AWS_BUCKET, backup.Name+"/"+runTime+".sql", fileName, minio.PutObjectOptions{ContentType: "application/sql"})
		if err != nil {
			log.Println("Error uploading file to S3", err)
			continue
		} else {
			log.Printf("Successfully uploaded %s to S3\n", backup.Name+"/"+runTime+".sql")
		}
		// Cleanup
		err = os.Remove(fileName)
		if err != nil {
			log.Println("Error removing file", fileName, "-", err)
		}
		log.Printf("Backup complete. Uploaded to %s at %s", backup.S3.AWS_BUCKET, time.Now().Format(time.RFC3339))
	}
	return nil

}

// Uses inotify to watch the config file for changes, if it changes then we send a message to the channel to reload
func (c *Config) reloadWatcher(configFile string) {
	watcher, err := inotify.NewWatcher()
	if err != nil {
		log.Println(err)
		log.Println("Cannot watch config file for changes")
	}
	err = watcher.Watch(configFile)
	if err != nil {
		log.Println(err)
		log.Println("Error watching config file for changes")
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
		env        = flag.Bool("env", false, "Use environment variables instead of config file")
		configFile = flag.String("config", "/etc/mysql-s3/config.yaml", "Path to config file")
		duration   = flag.Duration("duration", 24*time.Hour, "Duration between backups")
		version    = flag.Bool("v", false, "Print version and exit")
		mysqldump  = flag.String("mysqldump", "/usr/bin/mysqldump", "Path to mysqldump binary")
		curVersion = string("0.1.8")
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
		go config.reloadWatcher(*configFile)
	}

	for {
		config.Backup(*mysqldump)
		log.Println("Backup complete, sleeping for", *duration, "before next backup")
		time.Sleep(*duration)
	}

}
