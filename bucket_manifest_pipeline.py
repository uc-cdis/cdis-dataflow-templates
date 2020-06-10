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
from google.cloud import storage

from bucket_manifest.bucket import get_bucket_manifest, compute_md5
from bucket_manifest.pub import pub

FILE_HEADERS = ["bucket", "key", "size", "md5"]

class ComputeMD5(beam.DoFn):
    def __init__(self, project_id, pub_topic):
        self.project_id = project_id
        self.pub_topic = pub_topic
        self._buffer = []

    def process(self, element):
        """
        """
        text_line = element.strip()
        words = text_line.split("\t")
        fi = dict(zip(FILE_HEADERS, words))
        fi["size"] = int(fi["size"])

        try:
            fi["md5"] = compute_md5(fi.get("bucket"), fi.get("key"))
        except Exception as e:
            fi["md5"] = str(e)
        my_str = "{}\t{}\t{}".format(fi["bucket"], fi["key"], fi["md5"])
        logging.info("publish message: {}".format(my_str))

        pub(self.project_id.get(), self.pub_topic.get(), str.encode(my_str))

class BucketManifestOptions(PipelineOptions):
    """
    Runtime Parameters given during template execution
    bucket, pub_sub and output parameters are necessary for execution of pipeline
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--bucket',
            type=str,
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--project_id',
            type=str,
            help='project_id topic')
        parser.add_value_provider_argument(
            '--pub_topic',
            type=str,
            help='pubsub topic')
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
    if 0==0:
        # Initialize runtime parameters as object
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(SetupOptions).save_main_session = True
        p = beam.Pipeline(options=pipeline_options)

        bucket_manifest_options = pipeline_options.view_as(BucketManifestOptions)
        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled

        # Beginning of the pipeline
        blob_list = get_bucket_manifest(bucket_manifest_options.bucket)
        lines = (
            p
            | beam.Create(blob_list))

        result = lines | "copy" >> beam.ParDo(ComputeMD5(bucket_manifest_options.project_id, bucket_manifest_options.pub_topic))
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--bucket",
            dest="bucket",
            default="dcf-integration-test",
            help="Input file to process.",
        )
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

        objects = get_bucket_manifest(known_args.bucket)
        lines = (
            p
            | beam.Create(objects))

        result = lines | "copy" >> beam.ParDo(ComputeMD5("dcf-integration", "giang-example-topic"))

    prog = p.run()
    prog.wait_until_finish()

# python bucket_manifest_pipeline.py --runner DataflowRunner     --project dcf-integration     --staging_location gs://dcf-dataflow-bucket/staging     --temp_location gs://dcf-dataflow-bucket/temp  --region us-east1 --output gs://dcf-dataflow-bucket/output --setup_file ./setup.py
if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()
