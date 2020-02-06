# goetl

This package was forked from the [dailyburn/ratchet](https://github.com/dailyburn/ratchet). When I left my job at Daily Burn, I was removed as a maintainer on the Ratchet project. Unfortunately it seems like that project is no longer being maintained, but I was still using the original code whenever I could.

goetl starts off with the `release/v3.0.0` tag from the Daily Burn repo, and also implements the [payload abstraction request](https://github.com/dailyburn/ratchet/issues/24). It stands on the original work of @stephenb

### A library for performing data pipeline / ETL tasks in Go.

The Go programming language's simplicity, execution speed, and concurrency support make it a great choice for building data pipeline systems that can perform custom ETL (Extract, Transform, Load) tasks. goetl is a library that is written 100% in Go, and let's you easily build custom data pipelines by writing your own Go code.

goetl provides a set of built-in, useful data processors, while also providing
an interface to implement your own. Conceptually, data processors are organized
into stages, and those stages are run within a pipeline.

Each data processor is receiving, processing, and then sending data to the next stage in the pipeline. All data processors are running in their own goroutine, so all processing is happening concurrently. Go channels are connecting each stage of processing, so the syntax for sending data will be intuitive for anyone familiar with Go.

## Getting Started

- Check out the full Godoc reference:
 [![GoDoc](https://godoc.org/github.com/teambenny/goetl?status.svg)](https://godoc.org/github.com/teambenny/goetl)
- Get goetl:
      go get github.com/teambenny/goetl

While not necessary, it may be helpful to understand
some of the pipeline concepts used within Ratchet's internals: https://blog.golang.org/pipelines

## Why would I use this?

goetl could be used anytime you need to perform some type of custom ETL. At Benny AI we use goetl mainly to handle extracting data from our application databases, transforming it into reporting-oriented formats, and then loading it into our dedicated reporting databases.

Another good use-case is when you have data stored in disparate locations that can't be easily tied together. For example, if you have some CSV data stored on S3, some related data in a SQL database, and want to combine them into a final CSV or SQL output.

In general, goetl tends to solve the type of data-related tasks that you end up writing a bunch of custom and difficult to maintain scripts to accomplish.
