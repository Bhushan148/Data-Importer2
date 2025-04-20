from flask import Flask, render_template, request, flash, redirect, url_for, session
import pandas as pd
import numpy as np
import os
import tempfile
from werkzeug.utils import secure_filename
import mysql.connector
from mysql.connector import Error as MySQLError
import psycopg2
from psycopg2 import sql as pgsql

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

class DatabaseManager:
    @staticmethod
    def test_connection(db_type, host, user, password, database, port):
        try:
            if db_type == 'mysql':
                conn = mysql.connector.connect(
                    host=host, user=user, password=password, database=database, port=port
                )
            else:
                conn = psycopg2.connect(
                    host=host, user=user, password=password, dbname=database, port=port
                )
            conn.close()
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    @staticmethod
    def get_db_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        else:
            return 'VARCHAR(255)'

    @staticmethod
    def create_table_and_upload_data(db_config, df, table_name):
        db_type = db_config['db_type']
        df.columns = [''.join(e for e in col if e.isalnum() or e == '_') for col in df.columns]

        if db_type == 'mysql':
            return DatabaseManager._upload_mysql(db_config, df, table_name)
        else:
            return DatabaseManager._upload_postgres(db_config, df, table_name)

    @staticmethod
    def _upload_mysql(config, df, table_name):
        conn = None
        try:
            conn = mysql.connector.connect(
                host=config['host'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                port=config['port']
            )
            cursor = conn.cursor()
            columns = [f"`{col}` {DatabaseManager.get_db_type(dtype)}" for col, dtype in df.dtypes.items()]
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    {', '.join(columns)},
                    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({placeholders})"
            data = [tuple(x) for x in df.replace({np.nan: None}).values]
            cursor.executemany(insert_query, data)
            conn.commit()
            return True, len(data)
        except MySQLError as e:
            return False, str(e)
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    @staticmethod
    def _upload_postgres(config, df, table_name):
        conn = None
        try:
            conn = psycopg2.connect(
                host=config['host'],
                user=config['user'],
                password=config['password'],
                dbname=config['database'],
                port=config['port']
            )
            cursor = conn.cursor()
            columns = [f"{col} {DatabaseManager.get_db_type(dtype)}" for col, dtype in df.dtypes.items()]
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    {', '.join(columns)},
                    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            cursor.execute(create_query)
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            data = [tuple(x) for x in df.replace({np.nan: None}).values]
            cursor.executemany(insert_query, data)
            conn.commit()
            return True, len(data)
        except Exception as e:
            return False, str(e)
        finally:
            if conn:
                cursor.close()
                conn.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/connect', methods=['POST'])
def connect():
    db_type = request.form['db_type']
    host = request.form['host']
    user = request.form['user']
    password = request.form['password']
    database = request.form['database']
    port = int(request.form['port'])

    if DatabaseManager.test_connection(db_type, host, user, password, database, port):
        session['db_config'] = {
            'db_type': db_type,
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port
        }
        flash({'type': 'success', 'title': 'Connection Successful!', 'message': f'Connected to {db_type.upper()} database successfully!'})
        return redirect(url_for('upload'))
    else:
        flash({'type': 'error', 'title': 'Connection Failed', 'message': f'Failed to connect to {db_type.upper()}. Please check your credentials and try again.'})
        return redirect(url_for('index'))

@app.route('/upload')
def upload():
    if 'db_config' not in session:
        flash({'type': 'error', 'title': 'Database Not Connected', 'message': 'Please connect to a database first.'})
        return redirect(url_for('index'))
    return render_template('upload.html')

@app.route('/process', methods=['POST'])
def process():
    if 'db_config' not in session:
        flash({'type': 'error', 'title': 'Database Not Connected', 'message': 'Please connect to a database first.'})
        return redirect(url_for('index'))

    if 'file' not in request.files:
        flash({'type': 'error', 'title': 'No File Selected', 'message': 'Please select a file to upload.'})
        return redirect(url_for('upload'))

    file = request.files['file']
    table_name = request.form['table_name']
    
    if file.filename == '':
        flash({'type': 'error', 'title': 'No File Selected', 'message': 'Please select a file to upload.'})
        return redirect(url_for('upload'))
    
    if not allowed_file(file.filename):
        flash({'type': 'error', 'title': 'Invalid File Type', 'message': 'Only CSV or Excel files are allowed.'})
        return redirect(url_for('upload'))

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        success, result = DatabaseManager.create_table_and_upload_data(session['db_config'], df, table_name)
        os.remove(filepath)
        if success:
            flash({'type': 'success', 'title': 'Upload Successful!', 'message': f'Successfully uploaded {result} records to table "{table_name}".'})
        else:
            flash({'type': 'error', 'title': 'Upload Failed', 'message': f'Error: {result}'})
    except Exception as e:
        flash({'type': 'error', 'title': 'Processing Error', 'message': f'An error occurred: {str(e)}'})
        if os.path.exists(filepath):
            os.remove(filepath)
    return redirect(url_for('upload'))

if __name__ == '__main__':
    app.run(debug=True)