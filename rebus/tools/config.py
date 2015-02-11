import json


def get_output_altering_options(config_txt):
    """
    :param config_txt: serialized json dictionary containing an agent's
    configuration. Its 'output_altering_options' key lists other keys that
    contain configuration parameters that influence the agent's output.

    Returns a string describing output altering options contained in the input
    config_txt parameter.
    """
    config = json.loads(config_txt)
    output_altering_config = {k: config[k] for k in
                              config['output_altering_options']}
    return json.dumps(output_altering_config)
