package cmd

import (
	"fmt"
	"os"

	"github.com/jakobeberhardt/container-bench/internal/benchmark"
	"github.com/jakobeberhardt/container-bench/internal/config"
	"github.com/jakobeberhardt/container-bench/internal/version"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	configFile    string
	verbose       bool
	printMetaData bool
	csvExport     string
)

var rootCmd = &cobra.Command{
	Use:   "container-bench",
	Short: "A comprehensive container benchmarking tool for Intel RDT performance analysis",
	Long: `container-bench is a tool for defining, running, and profiling container-based benchmarks
on Intel RDT-enabled machines to study the busy neighbor problem regarding memory bandwidth and caches.

The tool provides:
- YAML-based benchmark configuration
- Container orchestration using Docker
- Hardware-level profiling using Intel RDT and perf
- Data collection and storage in InfluxDB
- Pluggable scheduler implementations`,
	Run: runBenchmark,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "Path to benchmark configuration file (required)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")
	rootCmd.PersistentFlags().BoolVarP(&printMetaData, "print-meta-data", "m", false, "Print benchmark metadata at the end")
	rootCmd.PersistentFlags().StringVar(&csvExport, "csv", "", "Export DataFrames to CSV files in specified directory (e.g., ./csv/)")
	// Don't mark config as required globally, handle it per command
}

func Execute() error {
	return rootCmd.Execute()
}

func runBenchmark(cmd *cobra.Command, args []string) {
	// Check if config file is provided
	if configFile == "" {
		log.Fatal("Configuration file is required. Use --config or -c flag.")
	}

	// Set up logging
	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Info("Starting container-bench benchmark driver")

	// Load configuration
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		log.WithError(err).Fatal("Failed to load configuration")
		os.Exit(1)
	}

	// Set log level from config
	logLevel, err := log.ParseLevel(cfg.Benchmark.LogLevel)
	if err != nil {
		log.WithError(err).Warn("Invalid log level in config, using INFO")
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.WithField("config", configFile).Info("Configuration loaded successfully")

	// Create and run benchmark
	benchmarkRunner, err := benchmark.NewRunnerWithOptions(cfg, configFile, printMetaData, csvExport)
	if err != nil {
		log.WithError(err).Fatal("Failed to create benchmark runner")
		os.Exit(1)
	}

	if err := benchmarkRunner.Run(); err != nil {
		log.WithError(err).Fatal("Benchmark execution failed")
		os.Exit(1)
	}
}

func init() {
	// Add version command
	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version number of container-bench",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.GetVersionInfo())
		},
	}
	rootCmd.AddCommand(versionCmd)

	// Add validate command
	var validateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Validate benchmark configuration file",
		Run: func(cmd *cobra.Command, args []string) {
			if configFile == "" {
				log.Fatal("Configuration file is required")
			}
			
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				log.WithError(err).Fatal("Configuration validation failed")
			}
			
			log.WithField("benchmark", cfg.Benchmark.Name).Info("Configuration is valid")
		},
	}
	rootCmd.AddCommand(validateCmd)
}
