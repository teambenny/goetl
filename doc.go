/*
package goetl is a library for performing data pipeline / ETL tasks in Go.

The main construct in goetl is Pipeline. A Pipeline has a series of
PipelineStages, which will each perform some type of data processing, and
then send new data on to the next stage. Each PipelineStage consists of one
or more Processors, which are responsible for receiving, processing, and
then sending data on to the next stage of processing. DataProcessors each
run in their own goroutine, and therefore all data processing can be executing
concurrently.

Here is a conceptual drawing of a fairly simple Pipeline:

        +--Pipeline------------------------------------------------------------------------------------------+
        |                                                                       PipelineStage 3              |
        |                                                                      +---------------------------+ |
        |  PipelineStage 1                 PipelineStage 2          +-JSON---> |  CSVWriter                | |
        | +------------------+           +-----------------------+  |          +---------------------------+ |
        | |  SQLReader       +-JSON----> | Custom Processor      +--+                                        |
        | +------------------+           +-----------------------+  |          +---------------------------+ |
        |                                                           +-JSON---> |  SQLWriter                | |
        |                                                                      +---------------------------+ |
        +----------------------------------------------------------------------------------------------------+

In this example, we have a Pipeline consisting of 3 PipelineStages. The first stage has a Processor that
runs queries on a SQL database, the second is doing custom transformation
work on that data, and the third stage branches into 2 Processors, one
writing the resulting data to a CSV file, and the other inserting into another
SQL database.

In the example above, Stage 1 and Stage 3 are using built-in Processors
(see the "processors" package/subdirectory). However, Stage 2 is using a custom
implementation of Processor. By using a combination of built-in processors,
and supporting the writing of any Go code to process data, goetl makes
it possible to write very custom and fast data pipeline systems. See the
Processor documentation to learn more.

Since each Processor is running in its own goroutine, SQLReader can continue pulling and sending
data while each subsequent stage is also processing data. Optimally-designed pipelines
have processors that can each run in an isolated fashion, processing data without having
to worry about what's coming next down the pipeline.

All data payloads sent between Processors implement the etldata.Payload interface.
Built-in processors send data flows using the type etldata.JSON. This provides
a good balance of consistency and flexibility. See the "data" package for details
and helper functions for dealing with etldata.Payload and etldata.JSON. Another good
read for handling JSON data in Go is http://blog.golang.org/json-and-go.

Note that many of the concepts in goetl were taken from the Golang blog's post on
pipelines (http://blog.golang.org/pipelines). While the details discussed in that
blog post are largely abstracted away by goetl, it is still an interesting read and
will help explain the general concepts being applied.

Creating and Running a Basic Pipeline

There are two ways to construct and run a Pipeline. The first is a basic, non-branching
Pipeline. For example:

        +------------+   +-------------------+   +---------------+
        | SQLReader  +---> CustomTransformer +---> SQLWriter     |
        +------------+   +-------------------+   +---------------+

This is a 3-stage Pipeline that queries some SQL data in stage 1, does some custom data
transformation in stage 2, and then writes the resulting data to a SQL table in stage 3.
The code to create and run this basic Pipeline would look something like:

        // First initalize the Processors
        read := processors.NewSQLReader(db1, "SELECT * FROM source_table")
        transform := NewCustomTransformer() // (This would your own custom Processor implementation)
        write := processors.NewSQLWriter(db2, "destination_table")

        // Then create a new Pipeline using them
        pipeline := goetl.NewPipeline(read, transform, write)

        // Finally, run the Pipeline and wait for either an error or nil to be returned
        err := <-pipeline.Run()

Creating and Running a Branching Pipeline

The second way to construct a Pipeline is using a PipelineLayout. This method allows
for more complex Pipeline configurations that support branching between stages that
are running multiple DataProcessors. Here is a (fairly complex) example:

                                                                   +----------------------+
                                                            +------> SQLReader (Dynamic)  +--+
                                                            |      +----------------------+  |
                                                            |                                |
                         +---------------------------+      |      +----------------------+  |    +-----------+
                   +-----> SQLReader (Dynamic Query) +------+   +--> Custom Processor     +-------> CSVWriter |
    +-----------+  |     +---------------------------+      |   |  +----------------------+  |    +-----------+
    | SQLReader +--+                                     +------+                            |
    +-----------+  |     +---------------------------+   |  |      +----------------------+  |    +-----------+
                   +-----> Custom Processor          +------+------> Custom Processor     +--+  +-> SQLWriter |
                         +---------------------------+   |         +----------------------+     | +-----------+
                                                         |                                      |
                                                         |         +----------------------+     |
                                                         +---------> Passthrough          +-----+
                                                                   +----------------------+

This Pipeline consists of 4 stages where each Processor is choosing which Processors
in the subsequent stage should receive the data it sends. The SQLReader in stage 2, for example,
is sending data to only 2 processors in the next stage, while the Custom Processor in
stage 2 is sending its data to 3. The code for constructing and running a Pipeline like this
would look like:

        // First, initialize all the DataProcessors that will be used in the Pipeline
        query1 := processors.NewSQLReader(db1, "SELECT * FROM source_table")
        query2 := processors.NewSQLReader(db1, sqlGenerator1) // sqlGenerator1 would be a function that generates the query at run-time. See SQLReader docs.
        custom1 := NewCustomProcessor1()
        query3 := processors.NewSQLReader(db2, sqlGenerator2)
        custom2 := NewCustomProcessor2()
        custom3 := NewCustomProcessor3()
        passthrough := processors.NewPassthrough()
        writeMySQL := processors.NewSQLWriter(db3, "destination_table")
        writeCSV := processors.NewCSVWriter(file)

        // Next, construct and validate the PipelineLayout. Each DataProcessor
        // is inserted into the layout via calls to goetl.Do().
        layout, err := goetl.NewPipelineLayout(
                goetl.NewPipelineStage(
                        goetl.Do(query1).Outputs(query2),
                        goetl.Do(query1).Outputs(custom1),
                ),
                goetl.NewPipelineStage(
                        goetl.Do(query2).Outputs(query3, custom3),
                        goetl.Do(custom1).Outputs(custom2, custom3, passthrough),
                ),
                goetl.NewPipelineStage(
                        goetl.Do(query3).Outputs(writeCSV),
                        goetl.Do(custom2).Outputs(writeCSV),
                        goetl.Do(custom3).Outputs(writeCSV),
                        goetl.Do(passthrough).Outputs(writeMySQL),
                ),
                goetl.NewPipelineStage(
                        goetl.Do(writeCSV),
                        goetl.Do(writeMySQL),
                ),
        )
        if err != nil {
                // layout is invalid
                panic(err.Error())
        }

        // Finally, create and run the Pipeline
        pipeline := goetl.NewBranchingPipeline(layout)
        err = <-pipeline.Run()

This example is only conceptual, the main points being to explain the flexibility
you have when designing your Pipeline's layout and to demonstrate the syntax for
constructing a new PipelineLayout.

*/
package goetl
