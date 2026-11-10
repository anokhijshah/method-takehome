"""Unit tests for dispatcher job parsing and batch-failure handling (no AWS calls).

Run: .venv/bin/python -m unittest discover tests
"""
import json
import os
import sys
import unittest
from unittest import mock

os.environ.update(CLUSTER="c", TASK_DEFINITION="td", SUBNETS="s1,s2", SECURITY_GROUP="sg", AWS_DEFAULT_REGION="us-east-2")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dispatcher"))
import handler  # noqa: E402

OUT = ["--output", "json", "--output-file", handler.OUTPUT_FILE]


def job(command, job_id="job-1", cli="networkscan"):
    return json.dumps({"job_id": job_id, "cli": cli, "command": command})


class ParseJobTest(unittest.TestCase):
    def test_spec_example(self):
        job_id, args = handler.parse_job(job("discover port --top-ports 100 --target 128.199.158.128"))
        self.assertEqual(job_id, "job-1")
        self.assertEqual(args, ["discover", "port", "--top-ports", "100", "--target", "128.199.158.128"] + OUT)

    def test_strips_cli_name_and_user_output_flags(self):
        _, args = handler.parse_job(job("networkscan discover port --target x -o yaml --output-file=/tmp/a -f b"))
        self.assertEqual(args, ["discover", "port", "--target", "x"] + OUT)

    def test_rejects_invalid_jobs(self):
        for body in [
            "not json",
            json.dumps({"cli": "networkscan"}),
            job("-rf /", cli="rm"),
            job(""),
            job('discover "unclosed'),
            job("discover port", job_id="../../etc"),
            job("-o yaml"),
        ]:
            with self.subTest(body=body), self.assertRaises(handler.InvalidJob):
                handler.parse_job(body)


class HandlerTest(unittest.TestCase):
    def record(self, message_id, body):
        return {"messageId": message_id, "body": body}

    @mock.patch.object(handler, "ecs")
    def test_only_transient_failures_are_retried(self, ecs):
        ecs.run_task.side_effect = [
            {"tasks": [{"taskArn": "arn:task/1"}], "failures": []},
            Exception("ThrottlingException"),
        ]
        event = {"Records": [
            self.record("ok", job("discover port --target a", "j1")),
            self.record("throttled", job("discover port --target b", "j2")),
            self.record("invalid", job("x", cli="rm")),
        ]}
        resp = handler.handler(event, None)
        self.assertEqual(resp, {"batchItemFailures": [{"itemIdentifier": "throttled"}]})
        self.assertEqual(ecs.run_task.call_count, 2)  # invalid job never reaches ECS

    @mock.patch.object(handler, "ecs")
    def test_run_task_uses_job_id_for_idempotency_and_upload(self, ecs):
        ecs.run_task.return_value = {"tasks": [{"taskArn": "arn:task/1"}], "failures": []}
        handler.handler({"Records": [self.record("m", job("discover port --target a", "abc-123"))]}, None)
        kwargs = ecs.run_task.call_args.kwargs
        self.assertEqual(kwargs["clientToken"], "abc-123")
        self.assertEqual(kwargs["startedBy"], "abc-123")
        upload = kwargs["overrides"]["containerOverrides"][1]
        self.assertEqual(upload["environment"], [{"name": "JOB_ID", "value": "abc-123"}])

    @mock.patch.object(handler, "ecs")
    def test_run_task_failures_list_is_retried(self, ecs):
        ecs.run_task.return_value = {"tasks": [], "failures": [{"reason": "RESOURCE:CPU"}]}
        resp = handler.handler({"Records": [self.record("m", job("discover port --target a"))]}, None)
        self.assertEqual(resp, {"batchItemFailures": [{"itemIdentifier": "m"}]})


if __name__ == "__main__":
    unittest.main()
