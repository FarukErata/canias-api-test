from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
from datetime import datetime
from flask_swagger_ui import get_swaggerui_blueprint

from decimal import Decimal

import os
import pg8000.native

app = Flask(__name__)
CORS(app)  

SWAGGER_URL = '/swagger'  
API_URL = '/static/swagger.json' 

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Canias AI Test API"
    }
)


app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


def get_db_connection():
    try:
        
        user = os.environ.get('PGUSER')
        password = os.environ.get('PGPASSWORD')
        host = os.environ.get('POSTGRES_HOST')
        database = os.environ.get('PGDATABASE')

        
        if not user or not password or not host or not database:
            raise ValueError(
                f"Missing required database parameters: "
                f"user={bool(user)}, password={bool(password)}, "
                f"host={bool(host)}, database={bool(database)}"
            )

        
        port = int(os.environ.get('NEON_PORT', '5432'))
        
       
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



@app.route('/', methods=['GET'])
def home():
    return redirect('/swagger')


@app.route('/api/info', methods=['GET'])
def api_info():
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
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON'}), 400
            
        data = request.get_json()

        if 'TABLE' not in data or not data.get('TABLE'):
            return jsonify({'error': 'Missing required parameter: TABLE'}), 400
        
        table_name = data.get('TABLE')
        
        allowed_tables = ['IASSALHEAD', 'IASSALITEM', 'IASCUSTOMER','IASINVSTOCK' ,'IASMATBASIC']
        if table_name not in allowed_tables:
            return jsonify({'error': 'Invalid table name'}), 400
        
        table_required_filters = {
            'IASSALHEAD': ['DOCTYPE', 'DOCNUM'],
            'IASSALITEM': ['DOCTYPE', 'DOCNUM', 'DOCITEM', 'MATERIAL'],
            'IASCUSTOMER': ['CUSTOMER', 'CUSTNAME','CITY'],
            'IASINVSTOCK': ['MATERIAL', 'WAREHOUSE', 'STOCKPLACE', 'STEXT'],
            'IASMATBASIC': ['MATERIAL', 'SKUNIT', 'MATTYPE', 'NAME']
        }
        
        table_columns = {
            'IASSALITEM': ['id', 'DOCTYPE', 'DOCNUM', 'DOCITEM', 'REFDOCTYPE', 'REFDOCNUM', 'REFITEMNUM', 'MATERIAL', 'QUANTITY'],
            'IASSALHEAD': ['id', 'DOCTYPE', 'DOCNUM', 'VALIDFROM', 'VALIDUNTIL', 'ISOFFCHAR', 'ISORDCHAR', 'ISDELCHAR', 'ISINVCHAR', 'CUSTOMER'],
            'IASCUSTOMER': ['id', 'CUSTOMER', 'CUSTNAME','CITY'],
            'IASINVSTOCK': ['id', 'MATERIAL', 'AVAILSTOCK', 'WAREHOUSE', 'STOCKPLACE', 'STEXT'],
            'IASMATBASIC': ['id', 'MATERIAL', 'SKUNIT', 'MATTYPE', 'BRUTWEIGHT', 'NAME']
        } 

        actual_columns = table_columns.get(table_name, [])
        required_filters = table_required_filters.get(table_name, [])
        
        query = f'SELECT * FROM "{table_name}" WHERE 1=1'
        
        for column in required_filters:
            if column == 'DOCITEM' and 'DOCITEM' in data and isinstance(data['DOCITEM'], int) and data['DOCITEM'] != 0:
                query += f' AND "{column}" = {data["DOCITEM"]}'
            elif column in data and isinstance(data[column], str) and data[column] != "":
                safe_value = data[column].replace("'", "''")
                query += f' AND "{column}" LIKE \'%{safe_value}%\''
        
        print(f"Direct query: {query}")
        
        conn = get_db_connection()
        rows = conn.run(query)
        
        items = []
        for row in rows:
            item = {}
            for i, col in enumerate(actual_columns):
                if i < len(row):
                     if isinstance(row[i], Decimal):
                        item[col] = float(row[i])
                     else:
                        item[col] = row[i]
            items.append(item)

        
        return jsonify(items)
    
    except Exception as e:
        import traceback
        error_msg = str(e)
        trace = traceback.format_exc()
        return jsonify({
            'error': f'Database error: {error_msg}',
            'traceback': trace
        }), 500

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
        "schemes": ["https", "http"], 
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
                "post": {  
                    "summary": "Query database table with filters",
                    "produces": ["application/json"],
                    "consumes": ["application/json"],
                    "parameters": [
                        {
                            "in": "body",
                            "name": "body",
                            "description": "Query parameters",
                            "required": True, 
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


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Internal server error'}), 500

# No app.run() needed for Vercel
