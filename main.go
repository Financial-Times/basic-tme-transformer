package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/basic-tme-transformer/tme"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/Financial-Times/tme-reader/tmereader"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	_ "github.com/joho/godotenv/autoload"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/sethgrid/pester"
)

func main() {
	app := cli.App("basic-tme-transformer", "An API for transforming basic TME concepts to UPP")

	tmeUsername := app.String(cli.StringOpt{
		Name:   "tme-username",
		Value:  "",
		Desc:   "TME username used for http basic authentication",
		EnvVar: "TME_USERNAME",
	})
	tmePassword := app.String(cli.StringOpt{
		Name:   "tme-password",
		Value:  "",
		Desc:   "TME password used for http basic authentication",
		EnvVar: "TME_PASSWORD",
	})
	tmeToken := app.String(cli.StringOpt{
		Name:   "token",
		Value:  "",
		Desc:   "Token to be used for accessig TME",
		EnvVar: "TME_TOKEN",
	})
	tmeBaseURL := app.String(cli.StringOpt{
		Name:   "tme-base-url",
		Value:  "https://tme.ft.com",
		Desc:   "TME base url",
		EnvVar: "TME_BASE_URL",
	})
	maxRecords := app.Int(cli.IntOpt{
		Name:   "maxRecords",
		Value:  int(10000),
		Desc:   "Maximum records to be queried to TME",
		EnvVar: "MAX_RECORDS",
	})
	batchSize := app.Int(cli.IntOpt{
		Name:   "batchSize",
		Value:  int(10),
		Desc:   "Number of requests to be executed in parallel to TME",
		EnvVar: "BATCH_SIZE",
	})

	baseURL := app.String(cli.StringOpt{
		Name:   "base-url",
		Value:  "http://localhost:8080/transformers/",
		Desc:   "Base url",
		EnvVar: "BASE_URL",
	})
	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	cacheFileName := app.String(cli.StringOpt{
		Name:   "cache-file-name",
		Value:  "cache.db",
		Desc:   "Cache file name",
		EnvVar: "CACHE_FILE_NAME",
	})
	graphiteTCPAddress := app.String(cli.StringOpt{
		Name:   "graphiteTCPAddress",
		Value:  "",
		Desc:   "Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally)",
		EnvVar: "GRAPHITE_ADDRESS",
	})
	graphitePrefix := app.String(cli.StringOpt{
		Name:   "graphitePrefix",
		Value:  "",
		Desc:   "Prefix to use. Should start with content, include the environment, and the host name. e.g. content.test.public.content.by.concept.api.ftaps59382-law1a-eu-t",
		EnvVar: "GRAPHITE_PREFIX",
	})
	logMetrics := app.Bool(cli.BoolOpt{
		Name:   "logMetrics",
		Value:  false,
		Desc:   "Whether to log metrics. Set to true if running locally and you want metrics output",
		EnvVar: "LOG_METRICS",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "INFO",
		Desc:   "Log level",
		EnvVar: "LOG_LEVEL",
	})

	app.Action = func() {

		lvl, _ := log.ParseLevel(*logLevel)
		log.SetLevel(lvl)

		client := getResilientClient()
		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		//log.SetFormatter(&log.JSONFormatter{})
		log.WithFields(log.Fields{
			"tmeUsername": *tmeUsername,
			"tmePassword": *tmePassword,
			"tmeToken":    *tmeToken,
			"tmeBaseURL":  *tmeBaseURL,
		}).Info("TME credentials")

		modelTransformer := new(tme.Transformer)
		repos := make(map[string]tmereader.Repository)
		for k, v := range tme.EndpointTypeMappings {
			repos[k] = tmereader.NewTmeRepository(
				client,
				*tmeBaseURL,
				*tmeUsername,
				*tmePassword,
				*tmeToken,
				*maxRecords,
				*batchSize,
				v["taxonomy"].(string),
				v["source"].(tmereader.TmeSource),
				modelTransformer)
		}

		service := tme.NewService(repos, *cacheFileName, client, *baseURL, *maxRecords)

		th := tme.NewHandler(service)
		buildRoutes(th)

		if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
			log.Fatalf("Unable to start server: %v", err)
		}
	}
	app.Run(os.Args)
}

func getResilientClient() *pester.Client {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 32,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}
	c := &http.Client{
		Transport: tr,
		Timeout:   30 * time.Second,
	}
	client := pester.NewExtendedClient(c)
	client.Backoff = pester.ExponentialBackoff
	client.MaxRetries = 5
	client.Concurrency = 1

	return client
}

func buildRoutes(th *tme.Handler) {
	servicesRouter := mux.NewRouter()

	getFullHandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(th.HandleGetFullTaxonomy),
	}

	getSingleHandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(th.HandleGetSingleConcept),
	}

	countHandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(th.GetCount),
	}

	getIDsHandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(th.GetIDs),
	}

	servicesRouter.Handle("/transformers/{type}", getFullHandler)
	servicesRouter.Handle("/transformers/{type}/__count", countHandler)
	servicesRouter.Handle("/transformers/{type}/__ids", getIDsHandler)
	servicesRouter.Handle("/transformers/{type}/{uuid}", getSingleHandler)

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.Handle("/", monitoringRouter)
}
