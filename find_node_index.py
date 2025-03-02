import json


def find_node_index(node_id, json_file_path):
    """
    Find the index of a specific node in the JSON file.

    Args:
        node_id (str): The node identifier to find (e.g., "NODE1000")
        json_file_path (str): Path to the JSON file

    Returns:
        int: The index (0-based) of the node in the JSON file,
             or -1 if the node is not found
    """
    # Read the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Get the list of nodes (keys)
    node_keys = list(data.keys())

    # Try to find the index of the specified node
    try:
        index = node_keys.index(node_id)
        print(index)
    except ValueError:
        # Return -1 if the node wasn't found
        return -1


index = find_node_index("NODE1845", "node-list.json")