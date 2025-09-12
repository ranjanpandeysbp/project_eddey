from flask import Flask, jsonify, request
from flasgger import Swagger, swag_from

app = Flask(__name__)
swagger = Swagger(app)

# Simulate a database with a dictionary
items_db = {
    "apple": {"name": "apple", "description": "A crisp, red fruit."},
    "banana": {"name": "banana", "description": "A long, yellow fruit."},
    "cherry": {"name": "cherry", "description": "A small, red fruit."}
}

@app.route('/')
def index():
    return "Welcome to the CRUD API!"

# --- Read Operations ---

@app.route('/items', methods=['GET'])
@swag_from({
    'responses': {
        200: {
            'description': 'A list of all items',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'name': {'type': 'string'},
                        'description': {'type': 'string'}
                    }
                }
            }
        }
    }
})
def get_all_items():
    """Get a list of all items."""
    return jsonify(list(items_db.values()))

@app.route('/items/<string:name>', methods=['GET'])
@swag_from({
    'parameters': [
        {
            'name': 'name',
            'in': 'path',
            'type': 'string',
            'required': True,
            'description': 'The name of the item'
        }
    ],
    'responses': {
        200: {
            'description': 'Details of a specific item',
            'schema': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'description': {'type': 'string'}
                }
            }
        },
        404: {
            'description': 'Item not found'
        }
    }
})
def get_item(name):
    """Get details of a specific item."""
    item = items_db.get(name)
    if item:
        return jsonify(item)
    else:
        return jsonify({"message": "Item not found"}), 404

# --- Create Operation ---

@app.route('/items', methods=['POST'])
@swag_from({
    'parameters': [
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'description': {'type': 'string'}
                },
                'required': ['name', 'description']
            }
        }
    ],
    'responses': {
        201: {
            'description': 'Item created successfully',
            'schema': {
                'type': 'object',
                'properties': {
                    'message': {'type': 'string'},
                    'item': {
                        'type': 'object',
                        'properties': {
                            'name': {'type': 'string'},
                            'description': {'type': 'string'}
                        }
                    }
                }
            }
        },
        400: {
            'description': 'Invalid input'
        },
        409: {
            'description': 'Item already exists'
        }
    }
})
def create_item():
    """Create a new item."""
    data = request.get_json()
    if not data or 'name' not in data or 'description' not in data:
        return jsonify({"message": "Invalid input. 'name' and 'description' are required."}), 400

    name = data['name']
    description = data['description']

    if name in items_db:
        return jsonify({"message": f"Item '{name}' already exists"}), 409

    new_item = {"name": name, "description": description}
    items_db[name] = new_item
    return jsonify({"message": "Item created successfully", "item": new_item}), 201

# --- Update Operation ---

@app.route('/items/<string:name>', methods=['PUT'])
@swag_from({
    'parameters': [
        {
            'name': 'name',
            'in': 'path',
            'type': 'string',
            'required': True,
            'description': 'The name of the item to update'
        },
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'properties': {
                    'description': {'type': 'string'}
                },
                'required': ['description']
            }
        }
    ],
    'responses': {
        200: {
            'description': 'Item updated successfully',
            'schema': {
                'type': 'object',
                'properties': {
                    'message': {'type': 'string'},
                    'item': {
                        'type': 'object',
                        'properties': {
                            'name': {'type': 'string'},
                            'description': {'type': 'string'}
                        }
                    }
                }
            }
        },
        400: {
            'description': 'Invalid input'
        },
        404: {
            'description': 'Item not found'
        }
    }
})
def update_item(name):
    """Update an existing item."""
    if name not in items_db:
        return jsonify({"message": "Item not found"}), 404

    data = request.get_json()
    if not data or 'description' not in data:
        return jsonify({"message": "Invalid input. 'description' is required for update."}), 400

    items_db[name]['description'] = data['description']
    return jsonify({"message": f"Item '{name}' updated successfully", "item": items_db[name]}), 200

# --- Delete Operation ---

@app.route('/items/<string:name>', methods=['DELETE'])
@swag_from({
    'parameters': [
        {
            'name': 'name',
            'in': 'path',
            'type': 'string',
            'required': True,
            'description': 'The name of the item to delete'
        }
    ],
    'responses': {
        200: {
            'description': 'Item deleted successfully',
            'schema': {
                'type': 'object',
                'properties': {
                    'message': {'type': 'string'}
                }
            }
        },
        404: {
            'description': 'Item not found'
        }
    }
})
def delete_item(name):
    """Delete an item."""
    if name in items_db:
        del items_db[name]
        return jsonify({"message": f"Item '{name}' deleted successfully"}), 200
    else:
        return jsonify({"message": "Item not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)