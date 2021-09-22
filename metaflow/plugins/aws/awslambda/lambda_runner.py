import sys
import hashlib
import json

from threading import Thread
from metaflow.datastore.util.s3tail import S3Tail
from botocore.config import Config

from metaflow.metaflow_config import (
    LAMBDA_THROTTLE_RETRIES,
)

if sys.version_info > (3, 0):
    from typing import TYPE_CHECKING, Callable, Dict, Optional

    if TYPE_CHECKING:
        from metaflow.metaflow_environment import MetaflowEnvironment

from metaflow.plugins.aws.utils import build_task_command
from metaflow.plugins.aws.aws_client import get_aws_client
from metaflow.mflog import delayed_update_while
from metaflow.mflog.mflog import refine

LAMBDA_MAX_MEMORY_MB = 10240

from metaflow.metaflow_config import (
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    METADATA_SERVICE_HEADERS,
    METADATA_SERVICE_URL,
    DEFAULT_METADATA,
)


class LambdaRuntimeException(Exception):
    pass


class LogTailer(Thread):

    _stop_flag = False

    def __init__(
        self,
        stdout_location,  # type: str
        stderr_location,  # type: str
        echo,  # type: Callable[[str, bool], None]
        prefix,  # type: str
    ):
        """
        Parameters
        ----------
        echo : Callable[str]
            Printer function to call for each log line
        """
        self._stdout_tail = S3Tail(stdout_location)
        self._stderr_tail = S3Tail(stderr_location)

        self._echo = echo
        self._prefix = prefix
        super(LogTailer, self).__init__()
        self.daemon = True

    def stop_tail(self):
        self._stop_flag = True

    def _print_available_from_s3(self):
        try:
            for line in self._stdout_tail:
                line = refine(line, prefix=self._prefix)
                self._echo(line.strip().decode("utf-8", errors="replace"), False)
            for line in self._stderr_tail:
                line = refine(line, prefix=self._prefix)
                self._echo(line.strip().decode("utf-8", errors="replace"), True)
        except Exception as ex:
            self._echo("[ temporary error in fetching logs: %s ]" % ex, True)

    def run(self):
        delayed_update_while(
            condition=lambda: not self._stop_flag,
            update_fn=self._print_available_from_s3,
        )
        self._print_available_from_s3()


def lambda_name(memory_mb, timeout_seconds, image_uri):
    """ Construct lambda name based on hashed resources """
    res_hash = hashlib.sha256(
        str(memory_mb).encode("utf8")
        + str(timeout_seconds).encode("utf8")
        + image_uri.encode("utf8")
    ).hexdigest()[:10]

    # We append unhashed resource values here for a bit more debuggability
    return "-".join(["mflambda", res_hash, str(memory_mb), str(timeout_seconds)])


def ensure_lambda(
    name,  # type: str
    timeout_seconds,  # type: int
    memory_mb,  # type: int
    role_arn,
    image_uri,
    refresh_image,  # type: bool
    echo,  # type: Callable[[str, bool], None]
):
    # type: (...) -> str
    """
    Make sure Lambda for the step exists. If it does, but resources or timeout
    configuration is not what we expect, update the lambda configuration.

    Return lambda ARN.
    """

    client = get_aws_client("lambda")

    return _maybe_update_lambda(
        client,
        name,
        timeout_seconds,
        memory_mb,
        role_arn,
        image_uri,
        refresh_image,
        echo=echo,
    )


def _maybe_update_lambda(
    client,
    name,
    timeout_seconds,  # type: int
    memory_size,  # type: float
    role_arn,
    image_uri,
    refresh_image,  # type: bool
    echo,  # type: Callable[[str, bool], None]
):
    # type: (...) -> str
    """
    This does all the necessary AWS API Calls to update or create Lambda. Returns
    Lambda ARN.
    """
    try:
        # Get current function configuration
        info = client.get_function(FunctionName=name)

        # If refresh_image is set, force update function code.
        if refresh_image:
            echo("Updating image for %s.." % name, False)
            client.update_function_code(
                FunctionName=name,
                ImageUri=image_uri,
            )
            waiter = client.get_waiter("function_updated")
            waiter.wait(FunctionName=name)

        return info["Configuration"]["FunctionArn"]
    except client.exceptions.ResourceNotFoundException:
        # If function does not exist, create it
        echo("Creating worker Lambda %s..." % name, False)
        response = client.create_function(
            FunctionName=name,
            Role=role_arn,
            PackageType="Image",
            Timeout=timeout_seconds,
            MemorySize=memory_size,
            Code={
                "ImageUri": image_uri,
            },
        )
        waiter = client.get_waiter("function_active")
        waiter.wait(FunctionName=name)
        return response["FunctionArn"]


class LambdaRunner:
    def __init__(
        self,
        environment,  # type: "MetaflowEnvironment"
        lambda_arn,  # type: str
        name,  # type: str
    ):
        config = Config(
            retries={
                "max_attempts": LAMBDA_THROTTLE_RETRIES,
            }
        )
        self._lambda_client = get_aws_client("lambda", params=dict(config=config))
        self._environment = environment
        self._lambda_arn = lambda_arn
        self._name = name

    def run(
        self,
        step_name,
        step_cli,
        code_package_sha,
        code_package_url,
        code_package_ds,
        env,  # type: Dict[str, str]
        attrs,  # type: Dict[str, str]
        echo,  # type: Callable[[str, bool], None]
        stdout_location,  # type: str
        stderr_location,  # type: str
        flow_name,  # type: str
        attempt,  # type: str
        task_id,  # type: str
        run_id,  # type: str
    ):
        # type: (...) -> int

        lambda_event = {
            "args": build_task_command(
                code_package_url=code_package_url,
                environment=self._environment,
                step_name=step_name,
                step_cmds=[step_cli],
                flow_name=flow_name,
                task_id=task_id,
                attempt=attempt,
                run_id=run_id,
                stdout_path="$(pwd)/logs/stdout.log",
                stderr_path="$(pwd)/logs/stderr.log",
            ),
            "shell": False,
            "env": {
                "METAFLOW_DEFAULT_DATASTORE": "s3",
                "METAFLOW_CODE_DS": code_package_ds,
                "METAFLOW_CODE_SHA": code_package_sha,
                "METAFLOW_CODE_URL": code_package_url,
                "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
                "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
                "METAFLOW_USER": attrs["metaflow.user"],
            },
            "parameters": {},
        }

        if METADATA_SERVICE_URL:
            lambda_event["env"]["METAFLOW_SERVICE_URL"] = METADATA_SERVICE_URL
        if METADATA_SERVICE_HEADERS:
            lambda_event["env"]["METAFLOW_SERVICE_HEADERS"] = json.dumps(
                METADATA_SERVICE_HEADERS
            )
        if DEFAULT_METADATA:
            lambda_event["env"]["METAFLOW_DEFAULT_METADATA"] = DEFAULT_METADATA

        for name, value in env.items():
            lambda_event["env"][name] = value

        if attrs:
            for key, value in attrs.items():
                lambda_event["parameters"][key] = value

        log_tailer = LogTailer(
            stdout_location,
            stderr_location,
            echo=echo,
            prefix="[%s] " % self._name,
        )
        log_tailer.start()

        config = Config(
            retries={
                "max_attempts": 1,  # We don't want boto to auto-retry the invocation
                                    # instead we'll rely on our scheduling logic to
                                    # do the retries.
            },
            read_timeout=930, # 15m30s, enough for the max lambda duration.
                              # Without this, Invoke call may fail due to socket
                              # timeout.
        )
        lambda_invoke_client = get_aws_client("lambda", params=dict(config=config))

        result = lambda_invoke_client.invoke(
            FunctionName=self._lambda_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(lambda_event).encode("utf8"),
        )
        log_tailer.stop_tail()
        log_tailer.join()
        parsed_result = json.loads(result["Payload"].read())

        if "errorMessage" in parsed_result and "errorType" in parsed_result and parsed_result["errorType"].startswith(
            "Runtime."
        ):
            # Runtime error, no point in retrying so raise the exception
            raise LambdaRuntimeException(parsed_result["errorMessage"])
        elif "errorMessage" in parsed_result and "Task timed out" in parsed_result["errorMessage"]:
            # Lambda timeout, return non-zero code so that the task will be retried
            echo("[%s] %s " % (self._name, parsed_result["errorMessage"]), True)
            return 1
        return parsed_result["return_code"]
