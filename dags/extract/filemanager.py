from datetime import datetime
import os
import simplejson as json


def handle_filemanager_error(func):
    """
    A decorator function that helps handling filemanager error, and keep track on specific file name

    Args:
        func (function): The function to be decorated.

    Returns:
        wrapper: The decorated function.

    Raises:
        Exception: If there is an error during file operation.

    """
    
    def wrapper(*args, **kwargs):
        try:
            if "write" in func.__name__:
                date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                if dest_folder := args[0].pop(0).get("name"):
                    directory = f"data/{dest_folder}/"
                    os.makedirs(directory, exist_ok=True)
                    dest_file = f"{directory}{date}.json"
                    return func(dest_file, *args, **kwargs)
                raise Exception("Wrong request data format.")
            else:
                if os.path.exists(args[0]):
                    return func(*args)
                raise Exception(f"File {args[0]} does not exist.")
        except Exception as e:
            print(f"Exception during file operation:\n{e}")
    return wrapper


@handle_filemanager_error
def write_json(dest_file: str, data: dict) -> None:
    """
    Write data to a JSON file.

    Args:
        dest_file (str): The path to the JSON file.
        data (dict): The data to be written to the JSON file.
    
    Returns:
        None
    """
    if not data:
        return
    with open(dest_file, "w") as file:
        file.write(json.dumps(data, indent=4, sort_keys=True))


@handle_filemanager_error
def read_json(file_path: str) -> dict:
    """
    Read data from a JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The data read from the JSON file.
    """
    with open(file_path, "r") as file:
        return json.load(file)
