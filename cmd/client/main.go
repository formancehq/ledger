package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:          "ledger-client",
	Short:        "Client for interacting with Ledger v3 POC Raft cluster",
	Long:         "A CLI client for interacting with the Ledger v3 POC Raft cluster operations",
	SilenceUsage: true,
}

var (
	serverURL string
	debugMode bool
)

func init() {
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9000", "Server URL (e.g., http://localhost:9000)")
	rootCmd.PersistentFlags().BoolVar(&debugMode, "debug", false, "Enable debug mode to display HTTP requests and responses")

	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(clusterStateCmd)
	rootCmd.AddCommand(bucketsCmd)
}

// debugHTTPClient wraps an HTTP client to log requests and responses when debug mode is enabled
type debugHTTPClient struct {
	client http.Client
	debug  bool
}

func (c *debugHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if c.debug {
		// Log request
		fmt.Fprintf(os.Stderr, "\n=== HTTP Request ===\n")
		fmt.Fprintf(os.Stderr, "%s %s\n", req.Method, req.URL.String())
		
		// Log headers
		fmt.Fprintf(os.Stderr, "Headers:\n")
		for key, values := range req.Header {
			for _, value := range values {
				fmt.Fprintf(os.Stderr, "  %s: %s\n", key, value)
			}
		}
		
		// Log body if present
		if req.Body != nil {
			bodyBytes, err := io.ReadAll(req.Body)
			if err == nil {
				req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				if len(bodyBytes) > 0 {
					fmt.Fprintf(os.Stderr, "Body:\n")
					var prettyJSON bytes.Buffer
					if err := json.Indent(&prettyJSON, bodyBytes, "", "  "); err == nil {
						fmt.Fprintf(os.Stderr, "%s\n", prettyJSON.String())
					} else {
						fmt.Fprintf(os.Stderr, "%s\n", string(bodyBytes))
					}
				}
			}
		}
		fmt.Fprintf(os.Stderr, "===================\n\n")
	}

	// Execute request
	resp, err := c.client.Do(req)
	if err != nil {
		if c.debug {
			fmt.Fprintf(os.Stderr, "=== HTTP Error ===\n")
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			fmt.Fprintf(os.Stderr, "==================\n\n")
		}
		return nil, err
	}

	if c.debug {
		// Log response
		fmt.Fprintf(os.Stderr, "=== HTTP Response ===\n")
		fmt.Fprintf(os.Stderr, "Status: %s\n", resp.Status)
		
		// Log headers
		fmt.Fprintf(os.Stderr, "Headers:\n")
		for key, values := range resp.Header {
			for _, value := range values {
				fmt.Fprintf(os.Stderr, "  %s: %s\n", key, value)
			}
		}
		
		// Log body
		bodyBytes, err := io.ReadAll(resp.Body)
		if err == nil {
			resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			if len(bodyBytes) > 0 {
				fmt.Fprintf(os.Stderr, "Body:\n")
				var prettyJSON bytes.Buffer
				if err := json.Indent(&prettyJSON, bodyBytes, "", "  "); err == nil {
					fmt.Fprintf(os.Stderr, "%s\n", prettyJSON.String())
				} else {
					fmt.Fprintf(os.Stderr, "%s\n", string(bodyBytes))
				}
			}
		}
		fmt.Fprintf(os.Stderr, "====================\n\n")
	}

	return resp, nil
}

// newSDKClient creates a new SDK client with optional debug HTTP client
func newSDKClient() *client.Formance {
	opts := []client.SDKOption{
		client.WithServerURL(serverURL),
	}

	if debugMode {
		debugClient := &debugHTTPClient{
			client: http.Client{Timeout: 60 * time.Second},
			debug:  true,
		}
		opts = append(opts, client.WithClient(debugClient))
	}

	return client.New(opts...)
}

var snapshotCmd = &cobra.Command{
	Use:          "snapshot",
	Short:        "Create a Raft cluster snapshot",
	Long:         "Forces the creation of a Raft cluster snapshot on the leader node",
	RunE:         runSnapshot,
	SilenceUsage: true,
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Call the snapshot endpoint
	res, err := sdk.Cluster.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot failed: %w", err)
	}

	if res.SnapshotResponse != nil && res.SnapshotResponse.Data != nil && res.SnapshotResponse.Data.Message != nil {
		fmt.Println(*res.SnapshotResponse.Data.Message)
	} else {
		fmt.Println("Snapshot created successfully")
	}

	return nil
}

var clusterStateCmd = &cobra.Command{
	Use:          "cluster-state",
	Short:        "Get the current state of the Raft cluster",
	Long:         "Returns the current state of the Raft cluster, including the list of nodes and the current leader",
	RunE:         runClusterState,
	SilenceUsage: true,
}

func runClusterState(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Call the cluster state endpoint
	res, err := sdk.Cluster.GetClusterState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %w", err)
	}

	// Extract cluster state data
	clusterState := res.GetClusterStateResponse()
	if clusterState == nil || clusterState.Data == nil {
		fmt.Println("No cluster state data available")
		return nil
	}

	data := clusterState.Data

	// Print cluster state information
	fmt.Println("Cluster State")
	fmt.Println("=============")

	// Local node state
	if data.State != nil {
		fmt.Printf("Local Node State: %s\n", *data.State)
	}

	// Local node ID
	if data.LocalNode != nil {
		fmt.Printf("Local Node ID: %s\n", *data.LocalNode)
	}

	// Leader
	if data.Leader != nil && *data.Leader != "" {
		fmt.Printf("Leader: %s\n", *data.Leader)
	} else {
		fmt.Println("Leader: (none)")
	}

	// Nodes list
	fmt.Println("\nNodes:")
	if len(data.Nodes) == 0 {
		fmt.Println("  (no nodes)")
	} else {
		for i, node := range data.Nodes {
			nodeID := "N/A"
			if node.ID != nil {
				nodeID = *node.ID
			}
			nodeAddr := "N/A"
			if node.Address != nil {
				nodeAddr = *node.Address
			}
			nodeSuffrage := "N/A"
			if node.Suffrage != nil {
				nodeSuffrage = string(*node.Suffrage)
			}

			// Mark leader
			leaderMark := ""
			if data.Leader != nil && node.ID != nil && *data.Leader == *node.ID {
				leaderMark = " (LEADER)"
			}

			// Mark local node
			localMark := ""
			if data.LocalNode != nil && node.ID != nil && *data.LocalNode == *node.ID {
				localMark = " (LOCAL)"
			}

			fmt.Printf("  %d. ID: %s, Address: %s, Suffrage: %s%s%s\n",
				i+1, nodeID, nodeAddr, nodeSuffrage, leaderMark, localMark)
		}
	}

	return nil
}

var bucketsCmd = &cobra.Command{
	Use:          "buckets",
	Short:        "Manage buckets",
	Long:         "Commands for managing buckets in the cluster",
	SilenceUsage: true,
}

var (
	bucketName   string
	bucketDriver string
	bucketConfig string
)

var bucketsCreateCmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a new bucket",
	Long:         "Creates a new bucket with the specified name, driver, and configuration",
	RunE:         runCreateBucket,
	SilenceUsage: true,
}

func init() {
	bucketsCreateCmd.Flags().StringVar(&bucketName, "name", "", "Bucket name (required)")
	bucketsCreateCmd.Flags().StringVar(&bucketDriver, "driver", "", "Driver name (required, e.g., postgres, s3)")
	bucketsCreateCmd.Flags().StringVar(&bucketConfig, "config", "{}", "Driver configuration as JSON (default: {})")
	bucketsCreateCmd.MarkFlagRequired("name")
	bucketsCreateCmd.MarkFlagRequired("driver")

	bucketsDeleteCmd.Flags().StringVar(&deleteBucketName, "name", "", "Bucket name to delete (required)")
	bucketsDeleteCmd.MarkFlagRequired("name")

	bucketsCmd.AddCommand(bucketsCreateCmd)
	bucketsCmd.AddCommand(bucketsListCmd)
	bucketsCmd.AddCommand(bucketsDeleteCmd)
}

var bucketsDeleteCmd = &cobra.Command{
	Use:          "delete",
	Short:        "Delete a bucket",
	Long:         "Deletes a bucket with the specified name",
	RunE:         runDeleteBucket,
	SilenceUsage: true,
}

var deleteBucketName string

func runDeleteBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flag
	if deleteBucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Delete bucket request
	req := operations.DeleteBucketRequest{
		BucketName: deleteBucketName,
	}

	// Call the delete bucket endpoint
	res, err := sdk.Buckets.DeleteBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	// Extract response data
	deleteResponse := res.GetDeleteBucketResponse()
	if deleteResponse == nil || deleteResponse.Data == nil {
		fmt.Printf("Bucket %s deleted successfully\n", deleteBucketName)
		return nil
	}

	data := deleteResponse.Data
	if data.Message != nil {
		fmt.Println(*data.Message)
	} else {
		fmt.Printf("Bucket %s deleted successfully\n", deleteBucketName)
	}

	return nil
}

func runCreateBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if bucketName == "" {
		return fmt.Errorf("bucket name is required")
	}
	if bucketDriver == "" {
		return fmt.Errorf("driver is required")
	}

	// Parse config JSON
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(bucketConfig), &config); err != nil {
		return fmt.Errorf("invalid config JSON: %w", err)
	}

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Create bucket request
	req := operations.CreateBucketRequest{
		BucketName: bucketName,
		CreateBucketRequest: components.CreateBucketRequest{
			Driver: bucketDriver,
			Config: config,
		},
	}

	// Call the create bucket endpoint
	res, err := sdk.Buckets.CreateBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	// Extract response data
	bucketResponse := res.GetCreateBucketResponse()
	if bucketResponse == nil || bucketResponse.Data == nil {
		fmt.Println("Bucket created successfully")
		return nil
	}

	data := bucketResponse.Data
	fmt.Println("Bucket created successfully")
	if data.Name != nil {
		fmt.Printf("Name: %s\n", *data.Name)
	}
	if data.Driver != nil {
		fmt.Printf("Driver: %s\n", *data.Driver)
	}
	if data.Config != nil {
		fmt.Printf("Config: %v\n", data.Config)
	}

	return nil
}

var bucketsListCmd = &cobra.Command{
	Use:          "list",
	Short:        "List all buckets",
	Long:         "Returns a list of all buckets in the cluster",
	RunE:         runListBuckets,
	SilenceUsage: true,
}

func runListBuckets(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Call the list buckets endpoint
	res, err := sdk.Buckets.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	// Extract response data
	bucketsResponse := res.GetListBucketsResponse()
	if bucketsResponse == nil || bucketsResponse.Data == nil {
		fmt.Println("No buckets found")
		return nil
	}

	buckets := bucketsResponse.Data
	if len(buckets) == 0 {
		fmt.Println("No buckets found")
		return nil
	}

	// Print buckets list
	fmt.Println("Buckets:")
	fmt.Println("========")
	for i, bucket := range buckets {
		fmt.Printf("\n%d. ", i+1)
		if bucket.Name != nil {
			fmt.Printf("Name: %s\n", *bucket.Name)
		}
		if bucket.Driver != nil {
			fmt.Printf("   Driver: %s\n", *bucket.Driver)
		}
		if bucket.CreatedAt != nil {
			fmt.Printf("   Created At: %s\n", *bucket.CreatedAt)
		}
		if bucket.Config != nil {
			configJSON, err := json.MarshalIndent(bucket.Config, "   ", "  ")
			if err == nil {
				fmt.Printf("   Config: %s\n", string(configJSON))
			}
		}
	}

	return nil
}
