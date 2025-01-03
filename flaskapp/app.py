from flask import Flask, request, jsonify
import snowflake.connector
import os
from flask_cors import CORS
import requests
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

# Function to get a Snowflake connection
def get_db_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

# Route to handle user query, generate SQL, and fetch results
@app.route('/process-query', methods=['POST'])
def process_query(): 
    previous = "" 
    try:
        # Parse user query
        #request_data = request.get_json()
        #user_query = request_data.get('query')
        message_history = request.json.get('messages', [])
        print(f" $$$$$$$$$$$$$$$$ message_history: {message_history}")
       # if not user_query:
        #    return jsonify({"error": "Query is required."}), 1000
        
        if not message_history:
            return jsonify({"error": "No message history provided"}), 1000

        # Step 1: Send user query to GPT to generate SQL query
        system_message_1 = ()
        apikey = os.getenv('OPEN_AI_API')
        client = OpenAI(api_key=apikey)
        gpt_sql_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message_1}] + message_history,
            temperature=0.0)
    

        response_content = gpt_sql_response.choices[0].message.content.strip()
        print(f"###############   first response : {response_content}")

        if not response_content:
            return jsonify({"error": "GPT did not generate a valid SQL query."}), 500

        # Step 2: Execute the generated SQL query on Snowflake
        data = []
        if response_content.startswith("SELECT"):
            conn = get_db_connection()
            if not conn:
                return jsonify({"error": "Unable to connect to Snowflake."}), 500

            cursor = conn.cursor()
            cursor.execute(response_content)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
            if data == []:
                print("No data found in the table")
                cursor.execute("SELECT * FROM FN_RK1_TABLE")
                rows = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
            cursor.close()
            conn.close()
            print(f"###############################len of data : {(len(data))}")
        # Step 3: Send the resultant data to GPT for answering the user's query
        system_message_2 = ()
        
        #print(f"################# data: system_message_2: {system_message_2}")
        #print(system_message_2)
        
        gpt_answer_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message_2}] + message_history,
            temperature=0.1)
        

        final_response_content = gpt_answer_response.choices[0].message.content.strip()
        print(f"###############   final_response_content: {final_response_content}")
        return jsonify({"response": final_response_content})

    except Exception as e:
        print(f"Error during processing: {e}")
        return jsonify({"error": "An error occurred while processing the request."}), 500

if __name__ == '__main__':
    # Flask will run on port 5000 by default
    app.run(debug=True)