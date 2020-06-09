# -*- coding: utf-8 -*-

from __future__ import absolute_import
import argparse
import sys
import os

import logging
from time import sleep

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# from bucket_manifest.settings import FILE_HEADERS
from bucket_manifest.bucket import get_bucket_manifest

FILE_HEADERS = ["bucket", "file_name", "size"]

class RefactorDict(beam.DoFn):
    def __init__(self):
        self._buffer = []

    def process(self, element):
        """
        """
        text_line = element.strip()
        words = text_line.split("\t")
        fi = dict(zip(FILE_HEADERS, words))
        fi["size"] = int(fi["size"])

        return [(fi, "processed")]

def format_result(result):
  (fi, datalog) = result
  return "%s\t%s\t%d" % (
      fi.get("bucket"),
      fi.get("file_name"),
      fi.get("size")
  )

class ContactUploadOptions(PipelineOptions):
    """
    Runtime Parameters given during template execution
    path and organization parameters are necessary for execution of pipeline
    campaign is optional for committing to bigquery
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--path',
            type=str,
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--output',
            type=str,
            help='output')

def run(argv=None):
    """
    Pipeline entry point, runs the all the necessary processes
    - Read CSV file out as Dict
    - Format Dictionary
    - Commit to Firestore and/or BigQuery
    """
    if 1==0:
        # Initialize runtime parameters as object
        contact_options = PipelineOptions().view_as(ContactUploadOptions)

        pipeline_options = PipelineOptions()
        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        pipeline_options.view_as(SetupOptions).save_main_session = True
        # Beginning of the pipeline
        p = beam.Pipeline(options=pipeline_options)
        blob_list = get_bucket_manifest("dcf-integration-test")
    
        lines = (
            p
            | beam.Create(blob_list))

        result = lines | "copy" >> beam.ParDo(RefactorDict())
        formated_result = result | "format" >> beam.Map(format_result)
        formated_result | "write" >> WriteToText(contact_options.output)
    else:
        parser = argparse.ArgumentParser()
        # parser.add_argument(
        #     "--input",
        #     dest="input",
        #     default="./scripts/test_data.txt",
        #     help="Input file to process.",
        # )
        parser.add_argument(
            "--output",
            dest="output",
            required=True,
            help="Output file to write results to.",
        )
        known_args, pipeline_args = parser.parse_known_args(argv)
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options.view_as(SetupOptions).save_main_session = True
        p = beam.Pipeline(options=pipeline_options)

        lines = (
            p
            | beam.Create(["test_bucket\taaa\t10"]))
        
        result = lines | "copy" >> beam.ParDo(RefactorDict())
        formated_result = result | "format" >> beam.Map(format_result)
        formated_result | "write" >> WriteToText(known_args.output)

    prog = p.run()
    prog.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()
