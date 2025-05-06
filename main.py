from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
from datetime import datetime
from flask_swagger_ui import get_swaggerui_blueprint

import os
import pg8000.native

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure Swagger UI
SWAGGER_URL = '/swagger'  # URL for exposing Swagger UI
API_URL = '/static/swagger.json'  # Where to fetch the Swagger specification

# Create Swagger UI blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Canias AI Test API"
    }
)

# Register the Swagger blueprint
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


def get_db_connection():
    try:
        # Get connection parameters from environment variables
        user = os.environ.get('PGUSER')
        password = os.environ.get('PGPASSWORD')
        host = os.environ.get('POSTGRES_HOST')
        database = os.environ.get('PGDATABASE')

        # Use default port 5432 if not specified
        port = int(os.environ.get('NEON_PORT', '5432'))
        
        # Connect with direct parameters
        return pg8000.native.Connection(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            ssl_context=True,
            timeout=5
        )
    except Exception as e:
        print(f"Connection error: {e}")
        raise e
    

def query_to_dict_list(rows, columns):
    """Convert query results to a list of dictionaries."""
    result = []
    for row in rows:
        row_dict = {}
        for i, column in enumerate(columns):
            row_dict[column] = row[i]
        result.append(row_dict)
    return result



# Redirect root URL to Swagger UI
@app.route('/', methods=['GET'])
def home():
    return redirect('/swagger')

# API information endpoint
@app.route('/api/info', methods=['GET'])
def api_info():
    # List of available endpoints with descriptions
    endpoints = [
        {
            'path': '/',
            'method': 'GET',
            'description': 'Redirects to Swagger UI documentation'
        },
        {
            'path': '/swagger',
            'method': 'GET',
            'description': 'Swagger UI documentation'
        },
        {
            'path': '/health',
            'method': 'GET',
            'description': 'Health check endpoint'
        },
        {
            'path': '/api/salservice',
            'method': 'GET',
            'description': 'Get sal info'
        }
       
    ]
    
    return jsonify({
        'name': 'Canias AI Test API',
        'version': '1.0.0',
        'description': 'A simple RESTful API with NeonDB integration using pg8000',
        'base_url': request.url_root,
        'endpoints': endpoints,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health', methods=['GET'])
def health_check():
    try:
        # Check database connection as part of health check
        conn = get_db_connection()
        conn.run('SELECT 1')
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Get all items
@app.route('/api/salservice', methods=['GET'])
def get_items():
    try:
        data = request.get_json()

        if 'TABLE' not in data or not data.get('TABLE'):
            return jsonify({'error': 'Missing required parameter: TABLE'}), 400
        
        table_name = data.get('TABLE')

        query = f"SELECT * FROM {table_name} WHERE 1=1"
        params = []
        
        param_mapping = {
            'USERNAME': 'USERNAME',
            'PASSWORD': 'PASSWORD', 
            'DOCTYPE': 'DOCTYPE',
            'DOCNUM': 'DOCNUM',
            'DOCITEM': 'DOCITEM',
            'CUSTOMER': 'CUSTOMER',
            'CUSTNAME': 'CUSTNAME',
            'MATERIAL': 'MATERIAL',
            'QUANTITY': 'QUANTITY'
        }

        query=f"SELECT * FROM {table_name}"

        for param_name, column_name in param_mapping.items():
            if param_name in data and data[param_name] is not None:
                query += f" AND {column_name} = ?"
                params.append(data[param_name])

       
        

        conn = get_db_connection()
        result = conn.run(query)

        columns = [column[0] for column in result.description]

        items = query_to_dict_list(result, columns)

        return jsonify(items)
    
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500



# Serve the swagger.json file
@app.route('/static/swagger.json')
def serve_swagger_spec():
    swagger_spec = {
        "swagger": "2.0",
        "info": {
            "version": "1.0.0",
            "title": "Canias AI Test API",
            "description": "A simple RESTful API with NeonDB integration using pg8000"
        },
        "basePath": "/",
        "schemes": ["https"],
        "consumes": ["application/json"],
        "produces": ["application/json"],
        "paths": {
            "/": {
                "get": {
                    "summary": "Redirects to Swagger UI documentation",
                    "produces": ["application/json"],
                    "responses": {
                        "302": {
                            "description": "Redirect to Swagger UI"
                        }
                    }
                }
            },
            "/api/info": {
                "get": {
                    "summary": "Get API information and endpoints",
                    "produces": ["application/json"],
                    "responses": {
                        "200": {
                            "description": "Successful operation"
                        }
                    }
                }
            },
            "/health": {
                "get": {
                    "summary": "Health check endpoint",
                    "produces": ["application/json"],
                    "responses": {
                        "200": {
                            "description": "Successful operation"
                        }
                    }
                }
            },
            "/api/salservice": {
                "get": {
                    "summary": "Get all items",
                    "produces": ["application/json"],
                    "responses": {
                        "200": {
                            "description": "Successful operation"
                        }
                    }
                }
            }
        },
        "definitions": {
            "Params": {
                "type": "object",
                "properties": {
                    "servicename": {
                        "type": "string"
                    },
                    "username": {
                        "type": "string"
                    },
                    "password": {
                        "type": "string"
                    },
                    "created_at": {
                        "type": "string"
                    }
                }
            }
        }
    }
    return jsonify(swagger_spec)

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Internal server error'}), 500

# No app.run() needed for Vercel