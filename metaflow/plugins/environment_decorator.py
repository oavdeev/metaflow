import os

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException


class EnvironmentDecorator(StepDecorator):
    """
    Step decorator to add or update environment variables prior to the execution of your step.

    The environment variables set with this decorator will be present during the execution of the
    step.

    To use, annotate your step as follows:
    ```
    @environment(vars={'MY_ENV': 'value'})
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    vars : Dict
        Dictionary of environment variables to add/update prior to executing your step.
    """
    name = 'environment'
    defaults = {'vars': {}}
    variable_attributes = True

    def vars_dict(self):
        env_vars = {}
        if 'vars' in self.attributes and isinstance(self.attributes['vars'], dict):
            env_vars.update(self.attributes['vars'])
        for k,v in self.attributes.items():
            if isinstance(v, str):
                env_vars[k] = v
        return env_vars

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        cli_args.env.update(self.vars_dict().items())
