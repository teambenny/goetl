package processors

import (
	"database/sql"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
)

type redshiftManifest struct {
	Entries []redshiftManifestEntry `json:"entries"`
}

type redshiftManifestEntry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

// RedshiftWriter gets data into a Redshift table by first uploading data batches to S3.
// Once all data is uploaded to S3, the appropriate "COPY" command is executed against the
// database to import the data files.
//
// This processor is not set up to do any fancy merging; rather, it writes every row received
// to the table defined. An ideal use case is writing data to a temporary table that is later
// merged into your production dataset.
type RedshiftWriter struct {
	bucket          string
	config          *aws.Config
	tx              *sql.Tx
	prefix          string
	tableName       string
	manifestEntries []redshiftManifestEntry
	data            []string
	BatchSize       int
	Compress        bool
	manifestPath    string
	S3IamRole       string

	// If the file name should be a fixed width, specify that here.
	// Files uploaded to S3 will be zero-padded to this width.
	// Defaults to 10.
	FileNameWidth int
}

// NewRedshiftProcessor returns a reference to a new Redshift Processor
func NewRedshiftWriter(tx *sql.Tx, config *aws.Config, tableName, bucket, prefix string) *RedshiftWriter {
	p := RedshiftWriter{
		bucket:        bucket,
		config:        config,
		tx:            tx,
		prefix:        prefix,
		tableName:     tableName,
		BatchSize:     1000,
		Compress:      true,
		FileNameWidth: 10,
	}

	return &p
}

// ProcessData stores incoming data in a local var. Once enough data has been received (as defined
// by r.BatchSize), it will write a file out to S3 and reset the local var
func (r *RedshiftWriter) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	objects, err := d.Objects()
	etlutil.KillPipelineIfErr(err, killChan)

	for _, obj := range objects {
		dd, err := etldata.NewJSON(obj)
		etlutil.KillPipelineIfErr(err, killChan)
		r.data = append(r.data, string(dd.Bytes()))

		// Flush the data if we've hit the threshold of records
		if r.BatchSize > 0 && len(r.data) >= r.BatchSize {
			r.flushFiles(killChan)
		}
	}
}

// Finish writes any remaining records to a file on S3, creates the manifest file, and then
// kicks off the query to import the S3 files into the Redshift table
func (r *RedshiftWriter) Finish(outputChan chan etldata.Payload, killChan chan error) {
	r.flushFiles(killChan)
	r.createManifest(killChan)
	r.copyToRedshift(killChan)
}

func (r *RedshiftWriter) flushFiles(killChan chan error) {
	formatString := fmt.Sprintf("%%0%vv", r.FileNameWidth)
	fileSuffix := fmt.Sprintf(formatString, len(r.manifestEntries))
	fileName := fmt.Sprintf("%vfile.%v", r.prefix, fileSuffix)
	_, err := etlutil.WriteS3Object(r.data, r.config, r.bucket, fileName, "\n", r.Compress)
	etlutil.KillPipelineIfErr(err, killChan)

	if r.Compress {
		fileName += ".gz"
	}

	entry := redshiftManifestEntry{
		URL:       fmt.Sprintf("s3://%v/%v", r.bucket, fileName),
		Mandatory: true,
	}
	r.manifestEntries = append(r.manifestEntries, entry)

	r.data = nil
}

func (r *RedshiftWriter) createManifest(killChan chan error) {
	manifest := redshiftManifest{Entries: r.manifestEntries}
	manifestData, err := etldata.NewJSON(manifest)
	etlutil.KillPipelineIfErr(err, killChan)

	dd := []string{string(manifestData)}
	r.manifestPath = fmt.Sprintf("%vfile.manifest", r.prefix)
	_, err = etlutil.WriteS3Object(dd, r.config, r.bucket, r.manifestPath, "\n", false)
	etlutil.KillPipelineIfErr(err, killChan)
}

func (r *RedshiftWriter) copyToRedshift(killChan chan error) {
	err := etlutil.ExecuteSQLQueryTx(r.tx, r.copyQuery())
	etlutil.KillPipelineIfErr(err, killChan)
}

func (r *RedshiftWriter) copyQuery() string {
	compression := ""
	if r.Compress {
		compression = "GZIP"
	}

	var credentials string
	if r.S3IamRole != "" {
		credentials = fmt.Sprintf("aws_iam_role=%v", r.S3IamRole)
	} else if r.config.Credentials != nil {
		creds, err := r.config.Credentials.Get()
		if err == nil {
			if creds.AccessKeyID != "" && creds.SecretAccessKey != "" {
				credentials = fmt.Sprintf("aws_access_key_id=%v;aws_secret_access_key=%v", creds.AccessKeyID, creds.SecretAccessKey)
			}
		}
	}
	if credentials != "" {
		credentials = fmt.Sprintf("CREDENTIALS '%v'", credentials)
	}

	query := fmt.Sprintf(`
                COPY %v
                FROM 's3://%v/%v'
                REGION '%v'
                %v
                MANIFEST
                JSON 'auto'
                %v
        `, r.tableName, r.bucket, r.manifestPath, *r.config.Region, credentials, compression)

	return query
}

func (r *RedshiftWriter) String() string {
	return "RedshiftWriter"
}
