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

        # Check if required parameters are present
        if not user or not password or not host or not database:
            raise ValueError(
                f"Missing required database parameters: "
                f"user={bool(user)}, password={bool(password)}, "
                f"host={bool(host)}, database={bool(database)}"
            )

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
            'method': 'POST',
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


@app.route('/api/salservice', methods=['POST'])
def get_items():
    try:
        # Check if the request has JSON data
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON'}), 400
            
        data = request.get_json()
        print(f"Request data: {data}")

        if 'TABLE' not in data or not data.get('TABLE'):
            return jsonify({'error': 'Missing required parameter: TABLE'}), 400
        
        table_name = data.get('TABLE')
        
        # Where şartına koyulcak alanlar
        table_required_filters = {
            'IASSALHEAD': ['DOCTYPE', 'DOCNUM'],
            'IASSALITEM': ['DOCTYPE', 'DOCNUM', 'DOCITEM', 'MATERIAL'],
            'IASCUSTOMER': ['CUSTOMER', 'CUSTNAME']
        }
        
        required_filters = table_required_filters.get(table_name, [])
        
        # Build basic query without WHERE clause first
        query = f'SELECT * FROM "{table_name}"'
        manual_query = f'SELECT * FROM "{table_name}"'
        params = []
        
        # Add filters
        where_conditions = []
        where_manual_conditions = []
        for column in required_filters:
            # For numeric parameters like DOCITEM, check differently
            if column == 'DOCITEM' and 'DOCITEM' in data and isinstance(data['DOCITEM'], int) and data[column] != 0:
                where_conditions.append(f'"{column}" = ?')
                params.append(data['DOCITEM'])
                # Add to manual query with the value directly
                where_manual_conditions.append(f'"{column}" = {data["DOCITEM"]}')
            # For string parameters, only add if non-empty
            elif column in data and isinstance(data[column], str) and data[column] != "":
                where_conditions.append(f'"{column}" = ?')
                params.append(data[column])
                # Add to manual query with the value directly
                where_manual_conditions.append(f'"{column}" = \'{data[column]}\'')
        
        # Add WHERE clause only if there are conditions
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        # Add WHERE clause to manual query
        if where_manual_conditions:
            manual_query += " WHERE " + " AND ".join(where_manual_conditions)
        
        # Execute query
        conn = get_db_connection()
        rows = conn.run(query, params)
        
        # Try to get the actual column names
        columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = LOWER('{table_name}')
            ORDER BY ordinal_position
        """
        columns_result = conn.run(columns_query)
        db_columns = [col[0] for col in columns_result]
        
        # If we couldn't get columns from the database, use required_filters
        if not db_columns:
            db_columns = required_filters
            
        # We'll use the required columns for our response
        result_columns = required_filters
        
        # Convert to JSON format - safer approach
        items = []
        for row in rows:
            item = {}
            # Map each column safely
            for i, col_name in enumerate(result_columns):
                # Find the position of this column in the db_columns list if possible
                try:
                    db_index = db_columns.index(col_name)
                    # If this index exists in the row, use it
                    if db_index < len(row):
                        item[col_name] = row[db_index]
                    else:
                        item[col_name] = None
                except ValueError:
                    # Column not found in db_columns
                    # As a fallback, try to use positional mapping if the index is within range
                    if i < len(row):
                        item[col_name] = row[i]
                    else:
                        item[col_name] = None
            items.append(item)
        
        # Build safe debug info that won't cause errors
        debug_info = {
            "parameterized_query": query,
            "manual_query": manual_query,
            "params": params,
            "row_count": len(rows),
            "db_columns": db_columns,
            "result_columns": result_columns,
            "first_row_raw": str(rows[0]) if rows else None
        }
        
        # Don't try to create a dict from the row, just include the raw data
        
        return jsonify({
            "items": items,
            "debug": debug_info
        })
    
    except Exception as e:
        import traceback
        error_msg = str(e)
        trace = traceback.format_exc()
        print(f"Error: {error_msg}")
        print(f"Traceback: {trace}")
        return jsonify({
            'error': f'Database error: {error_msg}',
            'traceback': trace
        }), 500


@app.route('/static/swagger.json')
def serve_swagger_spec():
    swagger_spec = {
        "swagger": "2.0",  # This is correct and should be kept
        "info": {
            "version": "1.0.0",
            "title": "Canias AI Test API",
            "description": "A simple RESTful API with NeonDB integration using pg8000"
        },
        "basePath": "/",
        "schemes": ["https", "http"],  # Added http for local development
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
                        },
                        "500": {
                            "description": "Server error"
                        }
                    }
                }
            },
            "/api/salservice": {
                "post": {  # Changed from "get" to "post"
                    "summary": "Query database table with filters",
                    "produces": ["application/json"],
                    "consumes": ["application/json"],
                    "parameters": [
                        {
                            "in": "body",
                            "name": "body",
                            "description": "Query parameters",
                            "required": True,  # Changed from true to True for Python
                            "schema": {
                                "$ref": "#/definitions/SalServiceParams"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Successful operation"
                        },
                        "400": {
                            "description": "Bad request"
                        },
                        "500": {
                            "description": "Server error"
                        }
                    }
                }
            }
        },
        "definitions": {
            "SalServiceParams": {
                "type": "object",
                "required": ["TABLE"],
                "properties": {
                    "TABLE": {
                        "type": "string",
                        "description": "Table name to query"
                    },
                    "USERNAME": {
                        "type": "string"
                    },
                    "PASSWORD": {
                        "type": "string"
                    },
                    "DOCTYPE": {
                        "type": "string"
                    },
                    "DOCNUM": {
                        "type": "string"
                    },
                    "DOCITEM": {
                        "type": "integer"
                    },
                    "CUSTOMER": {
                        "type": "string"
                    },
                    "CUSTNAME": {
                        "type": "string"
                    },
                    "MATERIAL": {
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